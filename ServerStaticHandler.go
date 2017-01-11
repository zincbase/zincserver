package main

import (
	"net/http"
	"regexp"
)

type ServerStaticHandler struct {
	parentServer *Server
}

var staticPathRegexp *regexp.Regexp

func init() {
	staticPathRegexp = regexp.MustCompile("^/static/(.*)$")
}

func (this *ServerStaticHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// This is currently a stub!

	defer r.Body.Close()

	submatches := staticPathRegexp.FindStringSubmatch(r.URL.Path)
	if len(submatches) == 0 {
		http.Error(w, "Invalid request path", http.StatusBadRequest)
		return
	}

	http.ServeFile(w, r, submatches[1])	
}

func NewServerStaticHandler(parentServer *Server) *ServerStaticHandler {
	return &ServerStaticHandler{
		parentServer: parentServer,
	}
}