package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alexdzyoba/chproxy/config"
	"github.com/alexdzyoba/chproxy/log"
)

var testDir = "./temp-test-data"

func TestMain(m *testing.M) {
	log.SuppressOutput(true)
	retCode := m.Run()
	log.SuppressOutput(false)
	if err := os.RemoveAll(testDir); err != nil {
		log.Fatalf("cannot remove %q: %s", testDir, err)
	}
	os.Exit(retCode)
}

func TestServe(t *testing.T) {
	var testCases = []struct {
		name     string
		file     string
		testFn   func(t *testing.T)
		listenFn func() (net.Listener, chan struct{})
	}{
		{
			"routing",
			"testdata/http.yml",
			func(t *testing.T) {
				req, err := http.NewRequest(http.MethodOptions, "http://127.0.0.1:9090?query=asd", nil)
				checkErr(t, err)
				resp, err := http.DefaultClient.Do(req)
				checkErr(t, err)
				expectedAllowHeader := "GET,POST"
				if resp.Header.Get("Allow") != expectedAllowHeader {
					t.Fatalf("header `Allow` got: %q; expected: %q", resp.Header.Get("Allow"), expectedAllowHeader)
				}
				resp.Body.Close()

				req, err = http.NewRequest(http.MethodConnect, "http://127.0.0.1:9090?query=asd", nil)
				checkErr(t, err)
				resp, err = http.DefaultClient.Do(req)
				checkErr(t, err)
				expected := fmt.Sprintf("unsupported method %q", http.MethodConnect)
				checkResponse(t, resp.Body, expected)
				if resp.StatusCode != http.StatusMethodNotAllowed {
					t.Fatalf("unexpected status code: %d; expected: %d", resp.StatusCode, http.StatusMethodNotAllowed)
				}
				resp.Body.Close()

				resp = httpGet(t, "http://127.0.0.1:9090/foobar", http.StatusBadRequest)
				expected = fmt.Sprintf("unsupported path: \"/foobar\"")
				checkResponse(t, resp.Body, expected)
				resp.Body.Close()
			},
			startHTTP,
		},
		{
			"http request",
			"testdata/http.yml",
			func(t *testing.T) {
				httpGet(t, "http://127.0.0.1:9090?query=asd", http.StatusOK)
				httpGet(t, "http://127.0.0.1:9090/metrics", http.StatusOK)
			},
			startHTTP,
		},
		{
			"http gzipped POST request",
			"testdata/http.cache.yml",
			func(t *testing.T) {
				var buf bytes.Buffer
				zw := gzip.NewWriter(&buf)
				_, err := zw.Write([]byte("SELECT * FROM system.numbers LIMIT 10"))
				checkErr(t, err)
				zw.Close()
				req, err := http.NewRequest("POST", "http://127.0.0.1:9090", &buf)
				checkErr(t, err)
				req.Header.Set("Content-Encoding", "gzip")
				resp, err := http.DefaultClient.Do(req)
				checkErr(t, err)
				body, _ := ioutil.ReadAll(resp.Body)
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("unexpected status code: %d; expected: %d; body: %s", resp.StatusCode, http.StatusOK, string(body))
				}
				resp.Body.Close()
			},
			startHTTP,
		},
		{
			"http POST request",
			"testdata/http.yml",
			func(t *testing.T) {
				buf := bytes.NewBufferString("SELECT * FROM system.numbers LIMIT 10")
				req, err := http.NewRequest("POST", "http://127.0.0.1:9090", buf)
				checkErr(t, err)
				resp, err := http.DefaultClient.Do(req)
				checkErr(t, err)
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("unexpected status code: %d; expected: %d", resp.StatusCode, http.StatusOK)
				}
				resp.Body.Close()
			},
			startHTTP,
		},
		{
			"http gzipped POST execution time",
			"testdata/http.execution.time.yml",
			func(t *testing.T) {
				var buf bytes.Buffer
				zw := gzip.NewWriter(&buf)
				_, err := zw.Write([]byte("SELECT * FROM system.numbers LIMIT 1000"))
				checkErr(t, err)
				zw.Close()
				req, err := http.NewRequest("POST", "http://127.0.0.1:9090", &buf)
				checkErr(t, err)
				req.Header.Set("Content-Encoding", "gzip")
				resp, err := http.DefaultClient.Do(req)
				checkErr(t, err)
				if resp.StatusCode != http.StatusGatewayTimeout {
					t.Fatalf("unexpected status code: %d; expected: %d", resp.StatusCode, http.StatusGatewayTimeout)
				}
				resp.Body.Close()
			},
			startHTTP,
		},
		{
			"deny http",
			"testdata/http.deny.http.yml",
			func(t *testing.T) {
				resp := httpGet(t, "http://127.0.0.1:9090?query=asd", http.StatusForbidden)
				expected := "user \"default\" is not allowed to access via http"
				checkResponse(t, resp.Body, expected)
				resp.Body.Close()
			},
			startHTTP,
		},
		{
			"http cached gzipped deadline",
			"testdata/http.cache.deadline.yml",
			func(t *testing.T) {
				var buf bytes.Buffer
				zw := gzip.NewWriter(&buf)
				_, err := zw.Write([]byte("SELECT SLEEP"))
				checkErr(t, err)
				zw.Close()
				req, err := http.NewRequest("POST", "http://127.0.0.1:9090", &buf)
				checkErr(t, err)
				req.Header.Set("Content-Encoding", "gzip")

				cacheDir := "temp-test-data/cache_deadline"
				checkFilesCount(t, cacheDir, 0)

				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(100*time.Millisecond))
				defer cancel()
				req = req.WithContext(ctx)
				_, err = http.DefaultClient.Do(req)
				expErr := "context deadline exceeded"
				if !strings.Contains(err.Error(), "context deadline exceeded") {
					t.Fatalf("unexpected error: %s; expected: %s", err, expErr)
				}
				select {
				case <-fakeCHState.syncCH:
					// wait while chproxy will detect that request was canceled and will drop temp file
					time.Sleep(time.Millisecond * 200)
					checkFilesCount(t, cacheDir, 0)
				case <-time.After(time.Second * 5):
					t.Fatalf("expected deadline query to be killed")
				}
			},
			startHTTP,
		},
	}

	// Wait until CHServer starts.
	go startCHServer()
	startTime := time.Now()
	i := 0
	for i < 10 {
		if _, err := http.Get("http://127.0.0.1:8124/"); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
		i++
	}
	if i >= 10 {
		t.Fatalf("CHServer didn't start in %s", time.Since(startTime))
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			*configFile = tc.file
			ln, done := tc.listenFn()
			defer func() {
				if err := ln.Close(); err != nil {
					t.Fatalf("unexpected error while closing listener: %s", err)
				}
				<-done
			}()

			var c *cluster
			for _, cluster := range proxy.clusters {
				c = cluster
				break
			}
			var i int
			for {
				if i > 3 {
					t.Fatal("unable to find active hosts")
				}
				if h := c.getHost(); h != nil {
					break
				}
				i++
				time.Sleep(time.Millisecond * 10)
			}

			tc.testFn(t)
		})
	}
}

func checkFilesCount(t *testing.T, dir string, expectedLen int) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatalf("error while reading dir %q: %s", dir, err)
	}
	if expectedLen != len(files) {
		t.Fatalf("expected %d files in cache dir; got: %d", expectedLen, len(files))
	}
}

var tlsClient = &http.Client{Transport: &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}}

func startHTTP() (net.Listener, chan struct{}) {
	cfg, err := loadConfig()
	if err != nil {
		panic(fmt.Sprintf("error while loading config: %s", err))
	}
	if err = applyConfig(cfg); err != nil {
		panic(fmt.Sprintf("error while applying config: %s", err))
	}
	done := make(chan struct{})
	ln, err := net.Listen("tcp4", cfg.Server.HTTP.ListenAddr)
	if err != nil {
		panic(fmt.Sprintf("cannot listen for %q: %s", cfg.Server.HTTP.ListenAddr, err))
	}
	h := http.HandlerFunc(serveHTTP)
	go func() {
		listenAndServe(ln, h, config.TimeoutCfg{})
		close(done)
	}()
	return ln, done
}

func startCHServer() {
	http.ListenAndServe(":8124", http.HandlerFunc(fakeCHHandler))
}

func fakeCHHandler(w http.ResponseWriter, r *http.Request) {
	query, err := getFullQuery(r)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "error while reading query: %s", err)
		return
	}
	if len(query) == 0 && r.Method != http.MethodGet {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "got empty query for non-GET request")
		return
	}
	switch string(query) {
	case "SELECT ERROR":
		w.WriteHeader(http.StatusTeapot)
		fmt.Fprint(w, "DB::Exception\n")
	case "SELECT SLEEP":
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "foo")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		fakeCHState.sleep()

		fmt.Fprint(w, "bar")
	default:
		if strings.Contains(string(query), killQueryPattern) {
			fakeCHState.kill()
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "Ok.\n")
	}
}

var fakeCHState = &stateCH{
	syncCH: make(chan struct{}),
}

type stateCH struct {
	sync.Mutex
	inited bool
	syncCH chan struct{}
}

func (s *stateCH) isInited() bool {
	s.Lock()
	defer s.Unlock()
	return s.inited
}

func (s *stateCH) kill() {
	s.Lock()
	defer s.Unlock()
	if !s.inited {
		return
	}
	close(s.syncCH)
}

func (s *stateCH) sleep() {
	s.Lock()
	s.inited = true
	s.Unlock()
	<-s.syncCH
}

func TestReloadConfig(t *testing.T) {
	*configFile = "testdata/http.yml"
	if err := reloadConfig(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	*configFile = "testdata/foobar.yml"
	if err := reloadConfig(); err == nil {
		t.Fatal("error expected; got nil")
	}
}

func checkErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("unexpected erorr: %s", err)
	}
}

func checkResponse(t *testing.T, r io.Reader, expected string) {
	if r == nil {
		t.Fatal("unexpected nil reader")
	}
	response, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("unexpected err while reading: %s", err)
	}
	got := string(response)
	if !strings.Contains(got, expected) {
		t.Fatalf("got: %q; expected: %q", got, expected)
	}
}

func httpGet(t *testing.T, url string, statusCode int) *http.Response {
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("unexpected erorr while doing GET request: %s", err)
	}
	if resp.StatusCode != statusCode {
		t.Fatalf("unexpected status code: %d; expected: %d", resp.StatusCode, statusCode)
	}
	return resp
}
