package main

import (
	//"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"github.com/gorilla/websocket"
)

type ServerDatastoreHandler struct {
	parentServer *Server
}

var datastorePathRegexp *regexp.Regexp
var accessKeyRegexp *regexp.Regexp
var accessKeyReplacerRegexp *regexp.Regexp

func init() {
	datastorePathRegexp = regexp.MustCompile(`^/datastore/([a-zA-Z0-9_]*(\.config)?)$`)
	accessKeyRegexp = regexp.MustCompile(`^[0-9a-f]*$`)
}

func (this *ServerDatastoreHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse URL
	submatches := datastorePathRegexp.FindStringSubmatch(r.URL.Path)

	if len(submatches) == 0 {
		endRequestWithError(w, r, http.StatusBadRequest, errors.New("Invalid datastore request path, should be of the form '/datastore/[name]', where [name] may only contain the characters A-Z, a-z and 0-9."))
		return
	}

	datastoreName := submatches[1]

	// Get operations object for the requested datastores
	operations := this.parentServer.GetDatastoreOperations(datastoreName)

	// Parse request
	method := r.Method
	parsedQuery := r.URL.Query()
	accessKey := parsedQuery.Get("accessKey")

	var accessKeyHash string

	if len(accessKey) == 0 {
		accessKeyHash = ""
	} else {
		hash := sha1.Sum([]byte(accessKey))
		accessKeyHash = hex.EncodeToString(hash[0:])
	}

	// Print a log message if needed
	logLevel := this.parentServer.startupOptions.LogLevel

	if logLevel > 0 {
		secureURI := strings.Replace(r.RequestURI, "accessKey="+accessKey, "[accessKeyHash="+accessKeyHash+"]", 1)

		if logLevel >= 2 {
			message := "\n"
			message += "[" + r.RemoteAddr + "]: " + r.Method + " " + secureURI + "\n"

			for k, v := range r.Header {
				message += fmt.Sprintf("%s: %s\n", k, v)
			}

			this.parentServer.Log(message, 2)
		} else if logLevel == 1 {

			this.parentServer.Log("["+r.RemoteAddr+"]: "+r.Method+" "+secureURI, 1)
		}
	}

	// Check for too long datastore names
	if len(datastoreName) > 128 {
		endRequestWithError(w, r, http.StatusBadRequest, errors.New("Datastore name cannot be longer than 128 characters"))
		return
	}

	// Send CORS headers and handle the OPTIONS request method if needed
	origin := r.Header.Get("Origin")

	if origin != "" {
		originAllowed, _ := operations.GetBoolConfigValue("['datastore']['CORS']['origin']['*']['allowed']")

		if !originAllowed {
			originAllowed, _ = operations.GetBoolConfigValue("['datastore']['CORS']['origin']['"+origin+"']['allowed']")
		}

		if originAllowed {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}

		if r.Method == "OPTIONS" {
			w.Header().Set("Access-Control-Allow-Methods", "GET,HEAD,POST,PUT,DELETE,OPTIONS")
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	// Set cache control headers
	w.Header().Set("Cache-Control", "max-age=0")

	if !accessKeyRegexp.MatchString(accessKey) || (len(accessKey) != 0 && len(accessKey) != 32) {
		endRequestWithError(w, r, http.StatusBadRequest, errors.New("A non-empty access key must contain exactly 32 lowercase hexedecimal digits."))
		return
	}

	if method == "GET" && strings.ToLower(r.Header.Get("Upgrade")) == "websocket" {
		method = "WebSocket"
	} else if method == "HEAD" { // For the context of this function, a 'HEAD' request is treated the same as 'GET'
		method = "GET"
	}

	// Get master key hash
	masterKeyHash, _ := this.parentServer.GlobalConfig().GetString("['server']['masterKeyHash']")

	// Check authorization and rate limits
	if accessKeyHash != masterKeyHash {
		if datastoreName == ".config" {
			endRequestWithError(w, r, http.StatusForbidden, errors.New("The configuration datastore can only be accessed through the master key."))
			return
		}

		accessProfileName, err := operations.GetStringConfigValue("['datastore']['accessKeyHash']['"+accessKeyHash+"']")

		if err != nil {
			endRequestWithError(w, r, http.StatusForbidden, errors.New("Invalid access key."))
			return
		}

		profileForMethodPrefix := "['accessProfile']['" + accessProfileName + "']['method']['" + method + "']"
		methodAllowed, _ := operations.GetBoolConfigValue(profileForMethodPrefix + "['allowed']")

		if !methodAllowed {
			endRequestWithError(w, r, http.StatusForbidden, errors.New(fmt.Sprintf("The access key '%s' is not authorized for '%s' requests.", accessKey, method)))
			return
		}

		remoteHost, _, _ := net.SplitHostPort(r.RemoteAddr)
		clientID := accessKeyHash + "@" + remoteHost

		requestLimitInterval, _ := operations.GetInt64ConfigValue(profileForMethodPrefix + "['limit']['requests']['interval']")

		if requestLimitInterval > 0 {
			requestLimitCount, _ := operations.GetInt64ConfigValue(profileForMethodPrefix + "['limit']['requests']['count']")

			allowed := operations.rateLimiter.ProcessRequest(clientID, method, requestLimitInterval, requestLimitCount)

			if !allowed {
				endRequestWithError(w, r, http.StatusTooManyRequests, errors.New(fmt.Sprintf("Maximum request rate exceeded. The client identifier '%s' is limited to %d %s requests per %dms.", clientID, requestLimitCount, method, requestLimitInterval)))
				return
			}
		}

		// Check permissions for individual request parameters
		for paramKey, _ := range parsedQuery {
			if paramKey == "accessKey" {
				continue
			}

			paramAllowed, _ := operations.GetBoolConfigValue(profileForMethodPrefix + "['param']['" + paramKey + "']['allowed']")

			if !paramAllowed {
				endRequestWithError(w, r, http.StatusForbidden, errors.New(fmt.Sprintf("The access key '%s' does not provide the permission to use the parameter '%s' in %s requests.", accessKey, paramKey, method)))

				return
			}
		}
	}

	var err error

	// Now that all general security checks have passed, dispatch appropriate handler for the particular operations
	// requested

	switch method {
	case "GET": // 'HEAD' is also included here as the 'method' variable would be reassigned to 'GET' in that case
		err = this.handleGetOrHeadRequest(w, r, datastoreName, operations, parsedQuery)
	case "WebSocket":
		err = this.handleWebsocketRequest(w, r, datastoreName, operations, parsedQuery)
		err = nil
	case "POST":
		err = this.handlePostRequest(w, r, datastoreName, operations, parsedQuery)
	case "PUT":
		err = this.handlePutRequest(w, r, datastoreName, operations, parsedQuery)
	case "DELETE":
		err = this.handleDeleteRequest(w, r, datastoreName, operations, parsedQuery)
	default:
		endRequestWithError(w, r, http.StatusMethodNotAllowed, nil)
	}

	if err != nil {
		this.parentServer.Log(fmt.Sprint(err), 1)
		endRequestWithError(w, r, http.StatusInternalServerError, err)
	}
}

func (this *ServerDatastoreHandler) handleGetOrHeadRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperationsEntry, query url.Values) (err error) {
	updatedAfter, _ := strconv.ParseInt(query.Get("updatedAfter"), 10, 64)
	// (ParseInt returns 0 if string was empty or invalid)

	if updatedAfter < 0 {
		endRequestWithError(w, r, http.StatusBadRequest, errors.New("Timestamp threshold must be greater than 0"))

		return
	}

	operations.RLock()
	err = operations.LoadIfNeeded()

	if err != nil {
		operations.RUnlock()

		switch err.(type) {
		case *os.PathError:
			endRequestWithError(w, r, http.StatusNotFound, nil)
			return nil
		}

		return
	}

	lastModifiedTime := operations.LastModifiedTime()

	if query.Get("waitUntilNonempty") == "true" && updatedAfter >= lastModifiedTime {
		updateChannel := operations.updateNotifier.CreateUpdateNotificationChannel(updatedAfter)
		operations.RUnlock()

		<-updateChannel

		err = this.handleGetOrHeadRequest(w, r, datastoreName, operations, query)
		return
	}

	compactResponse := query.Get("compact") == "true"

	var resultReader io.Reader

	var readSize int64
	resultReader, readSize, err = operations.CreateReader(updatedAfter, compactResponse)
	if err != nil {
		operations.RUnlock()
		return err
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(readSize, 10))

	bodyShouldBeSent := r.Method == "GET"
	if bodyShouldBeSent {
		FileDescriptors.Increment(operations.file)
		defer FileDescriptors.Decrement(operations.file)
	}

	operations.RUnlock()

	w.WriteHeader(http.StatusOK)

	if bodyShouldBeSent {
		_, err = io.Copy(w, resultReader)
		if err != nil {
			return
		}
	}

	return
}

func (this *ServerDatastoreHandler) handleWebsocketRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperationsEntry, query url.Values) (err error) {
	updatedAfter, _ := strconv.ParseInt(query.Get("updatedAfter"), 10, 64)
	// (ParseInt returns 0 if string was empty or invalid)

	if updatedAfter < 0 {
		operations.RUnlock()

		endRequestWithError(w, r, http.StatusBadRequest, errors.New("Timestamp threshold must be greater than 0"))

		return
	}

	var websocketUpgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	ws, err := websocketUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	for {
		operations.RLock()
		err = operations.LoadIfNeeded()

		if err != nil {
			operations.RUnlock()
			ws.Close()
			return
		}

		lastModifiedTime := operations.LastModifiedTime()

		if updatedAfter >= lastModifiedTime {
			updateChannel := operations.updateNotifier.CreateUpdateNotificationChannel(updatedAfter)
			operations.RUnlock()

			<-updateChannel
			continue
		}

		var resultReader io.Reader
		var messageWriter io.WriteCloser

		resultReader, _, err = operations.CreateReader(updatedAfter, false)

		if err != nil {
			operations.RUnlock()
			ws.Close()
			return err
		}

		messageWriter, err = ws.NextWriter(websocket.BinaryMessage)
		if err != nil {
			operations.RUnlock()
			ws.Close()
			return err
		}

		FileDescriptors.Increment(operations.file)
		operations.RUnlock()

		_, err = io.Copy(messageWriter, resultReader)
		FileDescriptors.Decrement(operations.file)

		if err != nil {
			ws.Close()
			return err
		}

		messageWriter.Close()
		updatedAfter = lastModifiedTime
	}
}

func (this *ServerDatastoreHandler) handlePostRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperationsEntry, query url.Values) (err error) {
	serializedEntries, err := ReadCompleteStream(r.Body)
	if err != nil {
		return
	}

	// Commit transaction
	operations.Lock()
	err = operations.LoadIfNeeded()

	if err != nil {
		operations.Unlock()

		switch err.(type) {
		case *os.PathError:
			endRequestWithError(w, r, http.StatusNotFound, nil)
			err = nil
		}

		return
	}

	commitTimestamp, err := operations.CommitTransaction(serializedEntries)
	operations.Unlock()

	if err != nil {
		// Check for datastore too large errors and respond accordingly
		switch err.(type) {
		case DatastoreTooLargeErr:
			endRequestWithError(w, r, http.StatusForbidden, err)
			err = nil
		}

		return
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, fmt.Sprintf(`{"commitTimestamp": %d}`, commitTimestamp))

	return
}

func (this *ServerDatastoreHandler) handlePutRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperationsEntry, query url.Values) (err error) {
	serializedEntries, err := ReadCompleteStream(r.Body)
	if err != nil {
		return
	}

	// Rewrite the datastore
	operations.Lock()
	commitTimestamp, err := operations.Rewrite(serializedEntries)
	operations.Unlock()

	if err != nil {
		// Check for datastore too large errors and respond accordingly
		switch err.(type) {
		case DatastoreTooLargeErr:
			endRequestWithError(w, r, http.StatusForbidden, err)
			err = nil
		}

		return
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, fmt.Sprintf(`{"commitTimestamp": %d}`, commitTimestamp))

	return
}

func (this *ServerDatastoreHandler) handleDeleteRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperationsEntry, query url.Values) (err error) {
	if operations.name == ".config" {
		endRequestWithError(w, r, http.StatusMethodNotAllowed, errors.New("The global configuration datastore cannot be deleted."))
		return
	}

	operations.Lock()
	err = operations.Destroy()
	operations.Unlock()

	if err != nil {
		switch err.(type) {
		case *os.PathError, *os.LinkError:
			endRequestWithError(w, r, http.StatusNotFound, nil)
			return nil
		}

		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	return
}

func endRequestWithError(w http.ResponseWriter, r *http.Request, statusCode int, err error) {
	if err != nil {
		http.Error(w, err.Error(), statusCode)
	} else {
		http.Error(w, "", statusCode)
	}
}

func NewServerDatastoreHandler(parentServer *Server) *ServerDatastoreHandler {
	return &ServerDatastoreHandler{
		parentServer: parentServer,
	}
}
