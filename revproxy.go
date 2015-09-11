package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	_ "net/http/pprof"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/net/netutil"
)

func main() {
	params := struct {
		Addr    string
		Conf    string
		Prof    string
		MaxConn int
	}{
		Addr:    "0.0.0.0:8080",
		Conf:    "/etc/revproxy.json",
		MaxConn: 1000,
	}
	flag.StringVar(&params.Addr, "addr", params.Addr, "`address` to listen at")
	flag.StringVar(&params.Conf, "conf", params.Conf, "configuration `file` with mapping")
	flag.StringVar(&params.Prof, "prof", params.Prof, "`address` to expose profile data at")
	flag.IntVar(&params.MaxConn, "maxconn", params.MaxConn, "maximum number of connections to accept")
	flag.Parse()

	conf, err := readConfig(params.Conf)
	if err != nil {
		log.Fatal(err)
	}

	proxy, err := NewRevProxy(conf)
	if err != nil {
		log.Fatal(err)
	}

	ln, err := Listen(params.Addr, params.MaxConn)
	if err != nil {
		log.Fatal(err)
	}

	srv := &http.Server{
		Handler:      proxy,
		ReadTimeout:  65 * time.Second,
		WriteTimeout: 65 * time.Second,
	}
	if params.Prof != "" {
		go func() {
			log.Println(http.ListenAndServe(params.Prof, nil))
		}()
	}
	log.Fatal(srv.Serve(ln))
}

func Listen(addr string, maxconn int) (net.Listener, error) {
	if maxconn < 1 {
		return nil, errors.New("maxconn should be positive")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return netutil.LimitListener(ln, maxconn), nil
}

type RevProxy struct {
	backends map[string]*httputil.ReverseProxy
	buckets  map[string]chan struct{}
}

func NewRevProxy(conf Config) (*RevProxy, error) {
	if err := conf.validate(); err != nil {
		return nil, err
	}
	rp := &RevProxy{
		backends: make(map[string]*httputil.ReverseProxy),
		buckets:  make(map[string]chan struct{}),
	}
	transport := http.DefaultTransport
	transport.(*http.Transport).MaxIdleConnsPerHost = conf.MaxKeepalivesPerBackend
	for k, v := range conf.Mapping {
		if strings.HasPrefix(v, "/") {
			// destination is unix socket. Make a custom transport
			// which routes any requests into this socket via
			// custom dialer, construct fake destination url from
			// source domain itself
			dst, err := url.Parse("http://" + k)
			if err != nil {
				return nil, err
			}
			rp.buckets[k] = make(chan struct{}, conf.MaxConnsPerBackend)
			p := httputil.NewSingleHostReverseProxy(dst)
			v := v // shadow variable
			p.Transport = &http.Transport{
				Dial: func(network, addr string) (net.Conn, error) {
					return net.Dial("unix", v)
				},
			}
			rp.backends[k] = p
			continue
		}
		// treat destination as tcp
		dst, err := url.Parse(v)
		if err != nil {
			return nil, err
		}
		rp.buckets[k] = make(chan struct{}, conf.MaxConnsPerBackend)
		p := httputil.NewSingleHostReverseProxy(dst)
		p.Transport = transport
		rp.backends[k] = p
	}
	return rp, nil
}

func readConfig(name string) (Config, error) {
	f, err := os.Open(name)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	var conf Config
	dec := json.NewDecoder(f)
	if err := dec.Decode(&conf); err != nil {
		return Config{}, err
	}
	return conf, nil
}

type Config struct {
	MaxConnsPerBackend      int
	MaxKeepalivesPerBackend int
	Mapping                 map[string]string
}

func (c Config) validate() error {
	if c.MaxConnsPerBackend < 1 {
		return errors.New("MaxConnsPerBackend is too low")
	}
	if c.MaxKeepalivesPerBackend < 1 {
		return errors.New("MaxKeepalivesPerBackend is too low")
	}
	if len(c.Mapping) == 0 {
		return errors.New("no backends provided")
	}
	return nil
}

func (rp *RevProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p, ok := rp.backends[r.Host]
	if !ok {
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}
	bkt := rp.buckets[r.Host]
	select {
	case bkt <- struct{}{}:
		defer func() { <-bkt }()
		p.ServeHTTP(w, r)
	default:
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}
}
