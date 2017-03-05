package main

import (
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

// Declare the datastore handler object type
type ServerDatastoreHandler struct {
	parentServer *Server
}

// Datastore handler object constructor function
func NewServerDatastoreHandler(parentServer *Server) *ServerDatastoreHandler {
	return &ServerDatastoreHandler{
		parentServer: parentServer,
	}
}

// Declare helper regular expression objects
var datastorePathRegexp *regexp.Regexp
var accessKeyRegexp *regexp.Regexp

func init() {
	// Initialize helper regular expression objects
	datastorePathRegexp = regexp.MustCompile(`^/datastore/([a-zA-Z0-9_]*(\.config)?)$`)
	accessKeyRegexp = regexp.MustCompile(`^[0-9a-f]*$`)
}

// The main handler for all datastore requests
func (this *ServerDatastoreHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Extract the datastore name form the request path
	requestPathSubmatches := datastorePathRegexp.FindStringSubmatch(r.URL.Path)

	// If no valid datastore name was found
	if len(requestPathSubmatches) == 0 || len(requestPathSubmatches[1]) == 0 || len(requestPathSubmatches[1]) > 128 {
		// Log a message
		this.parentServer.Log(1, "["+r.RemoteAddr+"]: "+r.Method+" "+r.URL.Path+" <invalid path>")

		// Ensure that cross-origin requests will also be able to receive the error
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// End with an error
		endRequestWithError(w, r, http.StatusBadRequest, errors.New("Invalid datastore request path, should be of the form '/datastore/[name][.config?]', where [name] may only contain the characters A-Z, a-z and 0-9 and have length between 1 and 128 characters."))

		return
	}

	// Get target datastore name from the match results
	datastoreName := requestPathSubmatches[1]

	// Get a configuration snapshot for the datastore
	config, configLoadErr := this.parentServer.GetConfigSnapshot(datastoreName)

	if configLoadErr != nil {
		// Ensure that cross-origin requests will also be able to receive the error
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// End with an error
		endRequestWithError(w, r, http.StatusInternalServerError, configLoadErr)

		return
	}

	// Send CORS headers and handle the OPTIONS request method if needed
	origin := r.Header.Get("Origin")

	if origin != "" {
		originAllowed, _ := config.GetBool("['datastore']['CORS']['origin']['*']['allowed']")

		if !originAllowed {
			originAllowed, _ = config.GetBool("['datastore']['CORS']['origin']['" + origin + "']['allowed']")
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

	// Get request method
	method := r.Method

	// Normalize the method variable for WebSocket requests
	if method == "GET" && strings.ToLower(r.Header.Get("Upgrade")) == "websocket" {
		method = "WebSocket"
	}

	// Parse the query
	parsedQuery := r.URL.Query()

	// Get the access key included in the request
	accessKey := parsedQuery.Get("accessKey")

	// Calculate the hex representation of the access key hash
	var accessKeyHash string

	if len(accessKey) == 0 {
		accessKeyHash = ""
	} else {
		accessKeyHash = SHA1ToHex([]byte(accessKey))
	}

	// Print a log message if needed
	logLevel := this.parentServer.startupOptions.LogLevel

	if logLevel > 0 {
		secureURI := strings.Replace(r.RequestURI, "accessKey="+accessKey, "[accessKeyHash="+accessKeyHash+"]", 1)

		if logLevel >= 2 {
			message := "\n"
			message += "[" + r.RemoteAddr + "]: " + method + " " + secureURI + "\n"

			for k, v := range r.Header {
				message += fmt.Sprintf("%s: %s\n", k, v)
			}

			this.parentServer.Log(2, message)
		} else if logLevel == 1 {
			this.parentServer.Log(1, "["+r.RemoteAddr+"]: "+method+" "+secureURI)
		}
	}

	// For the rest of this function, a 'HEAD' request is treated the same as 'GET'
	if method == "HEAD" {
		method = "GET"
	}

	// Verify the access key has a valid length and character set
	if len(accessKey) > 0 && (len(accessKey) != 32 || !accessKeyRegexp.MatchString(accessKey)) {
		endRequestWithError(w, r, http.StatusBadRequest, errors.New("A non-empty access key must contain exactly 32 lowercase hexedecimal digits."))
		return
	}

	// Get master key hash
	masterKeyHash, _ := config.GetString_GlobalOnly("['server']['masterKeyHash']")

	// Check authorization and rate limits
	if accessKeyHash != masterKeyHash {
		if IsConfigDatastoreName(datastoreName) {
			endRequestWithError(w, r, http.StatusUnauthorized, errors.New("A configuration datastore can only be accessed through the master key."))
			return
		}

		// Find the access profile for the given access key hash
		accessProfileName, err := config.GetString("['datastore']['accessKeyHash']['" + accessKeyHash + "']")

		if err != nil {
			// If a configuration entry wasn't found for the given key, end with an error
			endRequestWithError(w, r, http.StatusUnauthorized, errors.New("Invalid access key."))
			return
		}

		// Check if the profile support the requested method
		profileForMethodPrefix := "['accessProfile']['" + accessProfileName + "']['method']['" + method + "']"
		methodAllowed, _ := config.GetBool(profileForMethodPrefix + "['allowed']")

		if !methodAllowed {
			// If the profile doesn't support this method, end with an error
			endRequestWithError(w, r, http.StatusForbidden, errors.New(fmt.Sprintf("The access key '%s' is not authorized for '%s' requests.", accessKey, method)))
			return
		}

		// Parse the host and port of the client's IP and combine them to a client ID string
		remoteHost, _, _ := net.SplitHostPort(r.RemoteAddr)
		clientID := accessKeyHash + "@" + remoteHost

		// Check request rate limits
		requestLimitInterval, _ := config.GetInt64(profileForMethodPrefix + "['limit']['requests']['interval']")

		if requestLimitInterval > 0 {
			// If an interval setting exists, check its corresponding count limit
			requestLimitCount, _ := config.GetInt64(profileForMethodPrefix + "['limit']['requests']['count']")

			// Use the rate limiter object to decide if the allowed request rate has been exceeded
			allowed := this.parentServer.rateLimiter.ProcessEvent(datastoreName, clientID, method, requestLimitInterval, requestLimitCount)

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

			paramAllowed, _ := config.GetBool(profileForMethodPrefix + "['param']['" + paramKey + "']['allowed']")

			if !paramAllowed {
				endRequestWithError(w, r, http.StatusForbidden, errors.New(fmt.Sprintf("The access key '%s' does not provide the permission to use the parameter '%s' in %s requests.", accessKey, paramKey, method)))

				return
			}
		}
	}

	// Get operations object for the target datastore
	operations := this.parentServer.GetDatastoreOperations(datastoreName)

	var err error

	// Now that all general security checks have passed, dispatch the appropriate handler for
	// the particular method requested
	switch method {
	case "GET": // 'HEAD' is also included here as the 'method' variable would be changed to 'GET' in that case
		err = this.handleGetOrHeadRequest(w, r, datastoreName, operations, parsedQuery)
	case "WebSocket": // This method string was converted from GET earlier, if the request had an upgrade to WebSocket
		err = this.handleWebsocketRequest(w, r, datastoreName, operations, parsedQuery)
		err = nil
	case "POST", "PUT":
		err = this.handlePostOrPutRequest(w, r, datastoreName, operations, parsedQuery, config)
	case "DELETE":
		err = this.handleDeleteRequest(w, r, datastoreName, operations, parsedQuery)
	default:
		endRequestWithError(w, r, http.StatusMethodNotAllowed, nil)
	}

	// If an error occured, and wasn't properly handled to end the request, end the request
	// with an 'Internal Server Error' response
	if err != nil {
		this.parentServer.Log(1, err)
		endRequestWithError(w, r, http.StatusInternalServerError, err)
	}
}

// Handles a GET or HEAD request
func (this *ServerDatastoreHandler) handleGetOrHeadRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperations, query url.Values) (err error) {
	// Parse the "updatedAfter" query parameter (ParseInt returns 0 if string was empty or invalid).
	updatedAfter, _ := strconv.ParseInt(query.Get("updatedAfter"), 10, 64)

	// If a negative value was given, error
	if updatedAfter < 0 {
		endRequestWithError(w, r, http.StatusBadRequest, errors.New("Timestamp threshold must be greater or equal to 0"))

		return
	}

	// Load the datastore if needed
	state, err := operations.LoadIfNeeded(true)

	// Handle any error that occured while trying to load the datastore
	if err != nil {
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
	lastModifiedTime := state.LastModifiedTime()

	// If 'waitUntilNonempty' parameter was requested, and the update time threshold is larger than
	// the last modified time, wait until matching data is available and only then return it
	if query.Get("waitUntilNonempty") == "true" && updatedAfter >= lastModifiedTime {
		state.Decrement()

		waitGroup := operations.UpdateNotifier.CreateUpdateNotification(updatedAfter)
		waitGroup.Wait()

		err = this.handleGetOrHeadRequest(w, r, datastoreName, operations, query)
		return
	}

	defer state.Decrement()

	// Create a datastore reader
	var resultReader io.Reader
	var readSize int64

	resultReader, readSize, err = operations.CreateReader(state, updatedAfter)
	if err != nil {
		return err
	}

	// Set headers for the response
	w.Header().Set("Cache-Control", "max-age=0")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(readSize, 10))

	// If the request had a GET method (HEAD would skip this), increment the file descriptor counter
	// for the datastore to ensure the descripor wouldn't be closed until the read operation is over
	bodyShouldBeSent := r.Method == "GET"

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

// Handles WebSocket upgrade requests
func (this *ServerDatastoreHandler) handleWebsocketRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperations, query url.Values) (err error) {
	// Parse the "updatedAfter" query parameter (ParseInt returns 0 if string was empty or invalid).
	updatedAfter, _ := strconv.ParseInt(query.Get("updatedAfter"), 10, 64)

	// If a negative value was given, error
	if updatedAfter < 0 {
		endRequestWithError(w, r, http.StatusBadRequest, errors.New("Timestamp threshold must be greater or equal to 0"))
		return
	}

	// Load the datastore if needed, to ensure it exists
	state, err := operations.LoadIfNeeded(false)

	// Handle any error that occured while trying to load the datastore
	if err != nil {
		switch err.(type) {
		// If the error was a "file not found error", end with a 404 Not Found status code
		case *os.PathError:
			endRequestWithError(w, r, http.StatusNotFound, nil)
			return nil
		}

		// Otherwise, the error would be reported as an internal server error
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

	// Handle messages sent by the client
	go func() {
		for {
			messageType, _, err := ws.NextReader()

			// If an error occurred while reading the next message
			if err != nil {
				// return the error
				return
			}

			switch messageType {
			// Immediately terminate the connection if the client sends a binary or text message
			case websocket.BinaryMessage, websocket.TextMessage:
				ws.Close()
				return
				//case websocket.PingMessage:
				//	Log("ping")
				//case websocket.PongMessage:
				//	Log("pong")
			}
		}
	}()

	for {
		// Load the datastore if needed and increment reference count
		state, err = operations.LoadIfNeeded(true)

		// If an error ocurred loading the datastore
		if err != nil {
			// Close the WebSocket connection (no way to return an error to the user)
			ws.Close()
			return
		}

		// Get the time the datastore was last modified
		lastModifiedTime := state.LastModifiedTime()

		// If the requested update time threshold is equal or greater than the last modification time
		if updatedAfter >= lastModifiedTime {
			// Decrement reference count
			state.Decrement()

			// Wait until data is available
			waitGroup := operations.UpdateNotifier.CreateUpdateNotification(updatedAfter)
			waitGroup.Wait()

			continue
		}

		// Create a datastore reader
		var resultReader io.Reader
		var messageWriter io.WriteCloser

		resultReader, _, err = operations.CreateReader(state, updatedAfter)

		// If an error ocurred creating the reader
		if err != nil {
			// Decrement reference count
			state.Decrement()

			// Close the connection
			ws.Close()
			return err
		}

		// Create a writer for a binary WebSocket message
		messageWriter, err = ws.NextWriter(websocket.BinaryMessage)

		// If creating the websocket writer failed
		if err != nil {
			// Decrement reference count
			state.Decrement()

			// Close the connection
			ws.Close()
			return err
		}

		// Stream the matching data to the WebSocket writer
		_, err = io.Copy(messageWriter, resultReader)

		// Decrement reference count
		state.Decrement()

		// If an error ocurred while streaming the data
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

func (this *ServerDatastoreHandler) handlePostOrPutRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperations, query url.Values, config *DatastoreConfigSnapshot) (err error) {
	// Read the entire request body to memory
	transactionBytes, err := ReadEntireStream(r.Body)
	if err != nil {
		return
	}

	// If request method was POST and the request body was empty
	if r.Method == "POST" && len(transactionBytes) == 0 {
		// End with an error
		endRequestWithError(w, r, http.StatusBadRequest, errors.New("No transaction data was included in the request body"))
		return
	}

	// Wait to enter the writer queue
	writerQueueToken := operations.WriterQueue.Enter()

	// Initialize file rewrite flag
	rewrite := r.Method == "PUT"

	// Load the datastore if needed
	state, err := operations.LoadIfNeeded(false)

	// If an error ocurred while loading the datastore
	if err != nil && !rewrite {
		// If the error was a 'file not found' error
		if _, ok := err.(*os.PathError); ok {
			// If the enabled the "create" query argument
			if query.Get("create") == "true" {
				rewrite = true
			} else {
				// Leave the writer queue
				operations.WriterQueue.Leave(writerQueueToken)

				// End with a "404 Not Found" status
				endRequestWithError(w, r, http.StatusNotFound, nil)
				err = nil

				return
			}
		} else {
			// Leave the writer queue
			operations.WriterQueue.Leave(writerQueueToken)

			// Any other error would be given as an internal server error
			return
		}
	}

	// Get the datastore size limit
	datastoreSizeLimit, _ := config.GetInt64("['datastore']['limit']['maxSize']")

	var initialDatastoreSize int64

	if rewrite {
		initialDatastoreSize = 0
	} else {
		initialDatastoreSize = state.Size()
	}

	// Make sure the transaction wouldn't cause it to exceed this limit
	if datastoreSizeLimit > 0 && initialDatastoreSize+int64(len(transactionBytes)) > datastoreSizeLimit {
		// Leave the writer queue
		operations.WriterQueue.Leave(writerQueueToken)

		// End the request with a 'forbidden' error
		endRequestWithError(w, r, http.StatusForbidden, ErrDatastoreSizeLimitExceeded{fmt.Sprintf("Datastore '%s' is limited to a maximum size of %d bytes", operations.Name, datastoreSizeLimit)})

		return
	}

	// Get the entry size limit
	datastoreEntrySizeLimit, _ := config.GetInt64("['datastore']['limit']['maxEntrySize']")

	// Get commit timestamp
	commitTimestamp := operations.GetCollisionFreeTimestamp(state)

	// Validate and prepare transaction: rewrite its commit timestamps and ensure transaction end mark
	// for last entry
	err = ValidateAndPrepareTransaction(transactionBytes, commitTimestamp, datastoreEntrySizeLimit)

	// If an error occurred while preparing the transaction
	if err != nil {
		// Leave the writer queue
		operations.WriterQueue.Leave(writerQueueToken)

		// Handle an unexpected end of stream error
		if err == io.ErrUnexpectedEOF {
			endRequestWithError(w, r, http.StatusBadRequest, errors.New("An unexpected end of stream was encountered while validating the given transaction"))
			err = nil
		} else { // Handle other errors
			switch err.(type) {
			// Check for entry rejected errors and respond with a bad request status
			case ErrEntryRejected:
				endRequestWithError(w, r, http.StatusBadRequest, err)
				err = nil

			// Check for datastore too large errors and respond with a forbidden request status
			case ErrDatastoreEntrySizeLimitExceeded:
				endRequestWithError(w, r, http.StatusForbidden, err)
				err = nil
			}
		}

		return
	}

	if rewrite { // If the request was a PUT request or a POST request but the file didn't exist and creation
				 // flag was enabled
		// Rewrite the datastore with the given data
		err = operations.Rewrite(transactionBytes, commitTimestamp)

		// If an error occured while commiting the transaction
		if err != nil {
			// Leave the writer queue
			operations.WriterQueue.Leave(writerQueueToken)

			// Otherwise, any error would be considered an internal server error
			return
		}
	} else { // Otherwise, if it was a regular POST request
		// Get the flush setting for this datastore
		flushEnabled, _ := config.GetBool("['datastore']['flush']['enabled']")

		// Get the maximum delay value for flushes
		maxFlushDelay, _ := config.GetInt64("['datastore']['flush']['maxDelay']")

		// Append the processed transaction bytes to the datastore
		err = operations.Append(transactionBytes, state, commitTimestamp, flushEnabled, maxFlushDelay)

		// If an error occured while appending the transaction
		if err != nil {
			// Leave the writer queue
			operations.WriterQueue.Leave(writerQueueToken)

			// Otherwise, any other error would be considered an internal server error
			return
		}

		// Read related configuration options for compaction
		compactionEnabled, _ := config.GetBool("['datastore']['compaction']['enabled']")

		if compactionEnabled == true {
			compactionMinSize, err1 := config.GetInt64("['datastore']['compaction']['minSize']")
			compactionMinGrowthRatio, err2 := config.GetFloat64("['datastore']['compaction']['minGrowthRatio']")
			compactionMinUnusedSizeRatio, err3 := config.GetFloat64("['datastore']['compaction']['minUnusedSizeRatio']")

			if err1 == nil && err2 == nil && err3 == nil {
				// Perform a compaction check and compact if needed
				_, err = operations.CompactIfNeeded(operations.State, compactionMinSize, compactionMinGrowthRatio, compactionMinUnusedSizeRatio)

				// If an error occurred while compacting
				if err != nil {
					// Leave the writer queue
					operations.WriterQueue.Leave(writerQueueToken)

					// Return the error
					return
				}
			}
		}
	}

	// Leave the writer queue
	operations.WriterQueue.Leave(writerQueueToken)

	// Set the response content type to JSON
	w.Header().Set("Content-Type", "application/json")
	// Write the header with a 200 OK status
	w.WriteHeader(http.StatusOK)
	// Write the commit timestamp to the response body within a JSON object
	_, err = io.WriteString(w, fmt.Sprintf(`{"commitTimestamp": %d}`, commitTimestamp))

	// Any error here would become an internal server error
	return
}

// Handles DELETE requests
func (this *ServerDatastoreHandler) handleDeleteRequest(w http.ResponseWriter, r *http.Request, datastoreName string, operations *DatastoreOperations, query url.Values) (err error) {
	// If the target datastore is the global configuration datastore, reject the request
	// with a "method not allowed" status
	if datastoreName == ".config" {
		endRequestWithError(w, r, http.StatusMethodNotAllowed, errors.New("The global configuration datastore cannot be deleted."))
		return
	}
	// Wait to enter the writer queue
	writerQueueToken := operations.WriterQueue.Enter()

	// Destroy the datastore
	err = operations.Destroy()

	// If an error ocurred while destroying the datastore
	if err != nil {
		operations.WriterQueue.Leave(writerQueueToken)

		switch err.(type) {
		// If the error was becuase the file doesn't exist, end with a 404 Not Found status
		case *os.PathError, *os.LinkError:
			endRequestWithError(w, r, http.StatusNotFound, nil)
			return nil
		}

		return
	}

	// Leave the writer queue
	operations.WriterQueue.Leave(writerQueueToken)

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
