package main

import (
	"net"
	"regexp"
)

var loopbackIPRegExp *regexp.Regexp

func init() {
	loopbackIPRegExp = regexp.MustCompile(`^localhost$|^127(?:\.[0-9]+){0,2}\.[0-9]+$|^(?:0*\:)*?:?0*1$`)
}

type ServerListener struct {
	parentServer    *Server
	wrappedListener net.Listener
	protocol        string
}

func NewServerListener(parentServer *Server, wrappedListener net.Listener, protocol string) *ServerListener {
	return &ServerListener{parentServer: parentServer, wrappedListener: wrappedListener, protocol: protocol}
}

func (this *ServerListener) Accept() (conn net.Conn, err error) {
	for {
		conn, err = this.wrappedListener.Accept()

		if err != nil {
			return
		}

		remoteHost, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
		loopbackOnly, _ := this.parentServer.GlobalConfig().GetBool("['server']['" + this.protocol + "']['loopbackOnly']")

		if loopbackOnly && !loopbackIPRegExp.MatchString(remoteHost) {
			conn.Write([]byte("HTTP/1.1 403 Forbidden\r\nConnection: Close\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 30\r\n\r\nIncoming host IP is forbidden."))
			conn.Close()
			continue
		} else if banned, _ := this.parentServer.bannedIPs[remoteHost]; banned == true {
			conn.Write([]byte("HTTP/1.1 403 Forbidden\r\nConnection: Close\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 45\r\n\r\nIncoming host IP has been temporarily banned."))
			conn.Close()
			continue
		} else {
			return
		}
	}
}

func (this *ServerListener) Close() error {
	return this.wrappedListener.Close()
}

func (this *ServerListener) Addr() net.Addr {
	return this.wrappedListener.Addr()
}
