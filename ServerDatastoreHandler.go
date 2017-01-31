package main

import (
	//"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type ServerDatastoreHandler struct {
	parentServer *Server
}

var datastorePathRegexp *regexp.Regexp
var accessKeyRegexp *regexp.Regexp

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
			originAllowed, _ = operations.GetBoolConfigValue("['datastore']['CORS']['origin']['" + origin + "']['allowed']")
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

	// Normalize the 'method' variable for WebSocket and HEAD requests
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

		// Find the access profile for the given access key hash
		accessProfileName, err := operations.GetStringConfigValue("['datastore']['accessKeyHash']['" + accessKeyHash + "']")

		if err != nil {
			// If a configuration entry wasn't found for the given key, end with an error
			endRequestWithError(w, r, http.StatusForbidden, errors.New("Invalid access key."))
			return
		}

		// Check if the profile support the requested method
		profileForMethodPrefix := "['accessProfile']['" + accessProfileName + "']['method']['" + method + "']"
		methodAllowed, _ := operations.GetBoolConfigValue(profileForMethodPrefix + "['allowed']")

		if !methodAllowed {
			// If the profile doesn't support this method, end with an error
			endRequestWithError(w, r, http.StatusForbidden, errors.New(fmt.Sprintf("The access key '%s' is not authorized for '%s' requests.", accessKey, method)))
			return
		}

		// Parse the host and port of the client's IP and combine them to a client ID string
		remoteHost, _, _ := net.SplitHostPort(r.RemoteAddr)
		clientID := accessKeyHash + "@" + remoteHost

		// Check request rate limits
		requestLimitInterval, _ := operations.GetInt64ConfigValue(profileForMethodPrefix + "['limit']['requests']['interval']")

		if requestLimitInterval > 0 {
			// If an interval setting exists, check its corresponding count limit
			requestLimitCount, _ := operations.GetInt64ConfigValue(profileForMethodPrefix + "['limit']['requests']['count']")

			// Use the rate limiter object to decide if the allowed request rate has been exceeded
			allowed := operations.rateLimiter.ProcessRequest(clientID, method, requestLimitInterval, requestLimitCount)

			// If the rate has been exceeded, end with an error
			if !allowed {
				endRequestWithError(w, r, http.StatusTooManyRequests, errors.New(fmt.Sprintf("Maximum request rate exceeded. The client identifier '%s' is limited to %d %s requests per %dms.", clientID, requestLimitCount, method, requestLimitInterval)))
				return
			}
		}

		// Check permissions for each individual request parameter in the query part of the request URI
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

	// Now that all general security checks have passed, dispatch the appropriate handler for
	// the particular method requested

	switch method {
	case "GET": // 'HEAD' is also included here as the 'method' variable would be changed to 'GET' in that case
		err = this.handleGetOrHeadRequest(w, r, datastoreName, operations, parsedQuery)
	case "WebSocket": // This method string was converted from GET earlier, if the request had an upgrade to WebSocket
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

	// If an error occured, and wasn't properly handled to end the request, end the request
	// with an 'Internal Server Error' response
	if err != nil {
		this.parentServer.Log(fmt.Sprint(err), 1)
		endRequestWithError(w, r, http.StatusInternalServerError, err)
	}
}

func (this *ServerDatastoreHandler) handleGetOrHeadRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperationsEntry, query url.Values) (err error) {
	// Parse the "updatedAfter" query parameter (ParseInt returns 0 if string was empty or invalid).
	updatedAfter, _ := strconv.ParseInt(query.Get("updatedAfter"), 10, 64)

	// If a negative value was given, error
	if updatedAfter < 0 {
		endRequestWithError(w, r, http.StatusBadRequest, errors.New("Timestamp threshold must be greater or equal to 0"))

		return
	}

	// Lock the datastore for reading
	operations.RLock()

	// Load the datastore if needed
	err = operations.LoadIfNeeded()

	// Handle any error that occured when trying to load the datastore
	if err != nil {
		operations.RUnlock()

		switch err.(type) {
		// If the error was a "file not found error", end with a 404 Not Found status code
		case *os.PathError:
			endRequestWithError(w, r, http.StatusNotFound, nil)
			return nil
		}

		// Otherwise, the error would be reported as an internal server error
		return
	}

	// Get the time that datastore was last modified
	lastModifiedTime := operations.LastModifiedTime()

	// If 'waitUntilNonempty' parameter was requested, and the updat time threshold is larger than
	// the last modified time, wait until matching data is available and only then return it
	if query.Get("waitUntilNonempty") == "true" && updatedAfter >= lastModifiedTime {
		updateChannel := operations.updateNotifier.CreateUpdateNotificationChannel(updatedAfter)
		operations.RUnlock()

		<-updateChannel

		err = this.handleGetOrHeadRequest(w, r, datastoreName, operations, query)
		return
	}

	// Create a datastore reader
	var resultReader io.Reader
	var readSize int64

	resultReader, readSize, err = operations.CreateReader(updatedAfter)
	if err != nil {
		operations.RUnlock()
		return err
	}

	// Set headers for the response
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(readSize, 10))

	// If the request had a GET method (HEAD would skip this), increment the file descriptor counter
	// for the datastore to ensure the descripor wouldn't be closed until the read operation is over
	bodyShouldBeSent := r.Method == "GET"
	if bodyShouldBeSent {
		FileDescriptors.Increment(operations.file)
		defer FileDescriptors.Decrement(operations.file)
	}

	// Unlock reader lock
	operations.RUnlock()

	// Write the header
	w.WriteHeader(http.StatusOK)

	// If the request had a GET method (HEAD would skip this), send the body of the request.
	if bodyShouldBeSent {
		_, err = io.Copy(w, resultReader)
		if err != nil {
			// Any error during the reading of the datastore would result in an internal server error
			return
		}
	}

	return
}

func (this *ServerDatastoreHandler) handleWebsocketRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperationsEntry, query url.Values) (err error) {
	// Parse the "updatedAfter" query parameter (ParseInt returns 0 if string was empty or invalid).
	updatedAfter, _ := strconv.ParseInt(query.Get("updatedAfter"), 10, 64)

	// If a negative value was given, error
	if updatedAfter < 0 {
		operations.RUnlock()

		endRequestWithError(w, r, http.StatusBadRequest, errors.New("Timestamp threshold must be greater or equal to 0"))

		return
	}

	// Create a WebSocket upgrader object
	var websocketUpgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// Upgrade the request to a WebSocket request
	ws, err := websocketUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	for {
		// Lock the datastore for reading
		operations.RLock()

		// Load the datastore if needed
		err = operations.LoadIfNeeded()

		// If an error ocurred loading the datastore
		if err != nil {
			// Unlock reader lock
			operations.RUnlock()

			// Close the WebSocket connection (no way to return an error to the user)
			ws.Close()
			return
		}

		// Get the time the datastore was last modified
		lastModifiedTime := operations.LastModifiedTime()

		// If the requested update time threshold is equal or greater than the last modification time
		if updatedAfter >= lastModifiedTime {
			// Wait until data is available
			updateChannel := operations.updateNotifier.CreateUpdateNotificationChannel(updatedAfter)
			operations.RUnlock()

			<-updateChannel
			continue
		}

		// Create a datastore reader
		var resultReader io.Reader
		var messageWriter io.WriteCloser

		resultReader, _, err = operations.CreateReader(updatedAfter)

		// If an error ocurred creating the reader
		if err != nil {
			// Unlock
			operations.RUnlock()
			// Close the connection
			ws.Close()
			return err
		}

		// Create a writer for a binary WebSocket message
		messageWriter, err = ws.NextWriter(websocket.BinaryMessage)

		// If creating the writer failed
		if err != nil {
			// Unlock
			operations.RUnlock()
			// Close the connection
			ws.Close()
			return err
		}

		// Increment the datastore's file descriptor
		FileDescriptors.Increment(operations.file)

		// Unlock the reader lock to the datastore
		operations.RUnlock()

		// Stream the matching data to the WebSocket writer
		_, err = io.Copy(messageWriter, resultReader)

		// Decrement the datastore's file desriptor
		FileDescriptors.Decrement(operations.file)

		// If an error ocurred when streaming the data
		if err != nil {
			// Close
			ws.Close()
			return err
		}

		// Close the websocket message writer object
		messageWriter.Close()

		// Set the update time threshold to the last modified time
		updatedAfter = lastModifiedTime
	}
}

func (this *ServerDatastoreHandler) handlePostRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperationsEntry, query url.Values) (err error) {
	// Read the entire request body to memory
	serializedEntries, err := ReadEntireStream(r.Body)
	if err != nil {
		return
	}

	// Lock the datastore for writing
	operations.Lock()

	// Load the datastore if needed
	err = operations.LoadIfNeeded()

	// If an error ocurred when loading the datastore
	if err != nil {
		// Unlock the datastore
		operations.Unlock()

		switch err.(type) {
		case *os.PathError:
			// If the error was a 'file not found' error, end with a "404 Not Found" status
			endRequestWithError(w, r, http.StatusNotFound, nil)
			err = nil
		}

		// Any other error would be given as an internal server error
		return
	}

	// Commit the transaction bytes given in the request body
	commitTimestamp, err := operations.CommitTransaction(serializedEntries)

	// Unlock the datastore
	operations.Unlock()

	// If an error occured when commiting the transaction
	if err != nil {
		// Check for datastore too large errors and respond accordingly
		switch err.(type) {
		case DatastoreTooLargeErr:
			endRequestWithError(w, r, http.StatusForbidden, err)
			err = nil
		}

		// Otherwise, any other error would be considered an internal server error
		return
	}

	// Set the response content type to JSON
	w.Header().Set("Content-Type", "application/json")
	// Write the header with a 200 OK status
	w.WriteHeader(http.StatusOK)
	// Write the commit timestamp to the response body within a JSON object
	_, err = io.WriteString(w, fmt.Sprintf(`{"commitTimestamp": %d}`, commitTimestamp))

	// Any error here would become an internal server error
	return
}

func (this *ServerDatastoreHandler) handlePutRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperationsEntry, query url.Values) (err error) {
	// Read the entire request body to memory	
	serializedEntries, err := ReadEntireStream(r.Body)
	if err != nil {
		return
	}

	// Lock the datastore for writing
	operations.Lock()
	// Rewrite the datastore with the given data
	commitTimestamp, err := operations.Rewrite(serializedEntries)
	// Unlock the datastore
	operations.Unlock()

	// If an error occure
	if err != nil {
		// Check for datastore too large errors and respond accordingly
		switch err.(type) {
		case DatastoreTooLargeErr:
			endRequestWithError(w, r, http.StatusForbidden, err)
			err = nil
		}

		// Any other error would become an internal server error
		return
	}

	// Set the response content type to JSON
	w.Header().Set("Content-Type", "application/json")
	// Write the header with a 200 OK status
	w.WriteHeader(http.StatusOK)
	// Write the commit timestamp to the response body within a JSON object
	_, err = io.WriteString(w, fmt.Sprintf(`{"commitTimestamp": %d}`, commitTimestamp))

	// Any error here would become an internal server error
	return
}

func (this *ServerDatastoreHandler) handleDeleteRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperationsEntry, query url.Values) (err error) {
	// If the target datastore is the global configuration datastore, reject the request
	// with a "method not allowed" status
	if operations.name == ".config" {
		endRequestWithError(w, r, http.StatusMethodNotAllowed, errors.New("The global configuration datastore cannot be deleted."))
		return
	}

	// Lock the datastore for writing
	operations.Lock()
	// Destroy the datastore
	err = operations.Destroy()
	// Unlock the datastore
	operations.Unlock()

	// If an error ocurred when destroying the datastore
	if err != nil {
		switch err.(type) {
		// If the error was becuase the file doesn't exist, end with a 404 Not Found status
		case *os.PathError, *os.LinkError:
			endRequestWithError(w, r, http.StatusNotFound, nil)
			return nil
		}

		return
	}

	// Set the response content type to plain text
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	// Write the header
	w.WriteHeader(http.StatusOK)

	// Return, the response would close by itself
	return
}

// End the given request with the given error
func endRequestWithError(w http.ResponseWriter, r *http.Request, statusCode int, err error) {
	if err != nil {
		http.Error(w, err.Error(), statusCode)
	} else {
		http.Error(w, "", statusCode)
	}
}

// Datastore Handler objec constructor function
func NewServerDatastoreHandler(parentServer *Server) *ServerDatastoreHandler {
	return &ServerDatastoreHandler{
		parentServer: parentServer,
	}
}
