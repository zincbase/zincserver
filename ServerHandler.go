package main

import (
	"net/http"
	"strings"
)

type ServerHandler struct {
	parentServer     *Server
	datastoreHandler *ServerDatastoreHandler
	staticHandler    *ServerStaticHandler
}

func (this *ServerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/datastore/") {
		this.datastoreHandler.ServeHTTP(w, r)
		/*
			} else if strings.HasPrefix(r.URL.Path, "/static/") {
				this.staticHandler.ServeHTTP(w, r)
		*/
	} else {
		this.parentServer.Log("["+r.RemoteAddr+"]: " + r.Method + " " + r.URL.Path + " <invalid path>", 1)
		w.Header().Set("Access-Control-Allow-Origin", "*")
		http.Error(w, "Invalid request path.", http.StatusBadRequest)
	}
}

func NewServerHandler(parentServer *Server) *ServerHandler {
	return &ServerHandler{
		parentServer:     parentServer,
		datastoreHandler: NewServerDatastoreHandler(parentServer),
		staticHandler:    NewServerStaticHandler(parentServer),
	}
}
