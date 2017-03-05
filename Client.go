package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

// The client object
type Client struct {
	hostURL       string
	datastoreName string
	accessKey     string
}

// The client object constructor
func NewClient(hostURL string, datastoreName string, accessKey string) *Client {
	return &Client{hostURL: hostURL, datastoreName: datastoreName, accessKey: accessKey}
}

// A type for the returned result in PUT and POST requests
type PutPostResponse struct {
	CommitTimestamp int64 `json:"commitTimestamp"`
}

// Sends a GET request to the server with the given 'updatedAfter' minimum timestamp
func (this *Client) Get(updatedAfter int64) (results []Entry, err error) {
	params := map[string]string{}

	if updatedAfter > 0 {
		params["updatedAfter"] = fmt.Sprintf("%d", updatedAfter)
	}
	_, responseBody, err := this.Request("GET", params, nil)
	if err != nil {
		return
	}

	results, err = DeserializeEntryStreamBytes(responseBody)

	return
}

// Sends a GET request to the server with the given 'updatedAfter' minimum timestamp
func (this *Client) GetWhenNonEmpty(updatedAfter int64) (results []Entry, err error) {
	params := map[string]string{
		"updatedAfter":      fmt.Sprintf("%d", updatedAfter),
		"waitUntilNonempty": "true",
	}

	_, responseBody, err := this.Request("GET", params, nil)
	if err != nil {
		return
	}

	results, err = DeserializeEntryStreamBytes(responseBody)

	return
}

// Sends a GET request to the server with the given 'updatedAfter' minimum timestamp, and compacts the results
func (this *Client) GetAndCompact(updatedAfter int64) (results []Entry, err error) {
	params := map[string]string{}

	if updatedAfter > 0 {
		params["updatedAfter"] = fmt.Sprintf("%d", updatedAfter)
	}
	_, responseBody, err := this.Request("GET", params, nil)
	if err != nil {
		return
	}

	compactedEntryStream, err := CompactEntryStreamBytes(responseBody)
	if err != nil {
		return
	}

	results, err = DeserializeEntryStreamBytes(compactedEntryStream)
	if err != nil {
		return
	}

	return
}

// Sends a GET request to the server with the given 'updatedAfter' minimum timestamp, and compacts the results
func (this *Client) GetAndCompact2(updatedAfter int64) (results []Entry, err error) {
	results, err = this.Get(updatedAfter)

	if err != nil {
		return
	}

	return CompactEntries(results), nil
}

// Sends a POST request to the server with the given entries as a transaction
func (this *Client) Post(entries []Entry) (commitTimestamp int64, err error) {
	serializedEntriesBytes := SerializeEntries(entries)
	_, responseBody, err := this.Request("POST", map[string]string{}, bytes.NewReader(serializedEntriesBytes))

	if err != nil {
		return
	}

	responseObject := PutPostResponse{}

	err = json.Unmarshal(responseBody, &responseObject)
	if err != nil {
		return
	}

	return responseObject.CommitTimestamp, nil
}

// Sends a PUT request to the server with the given entries as a transaction
func (this *Client) Put(entries []Entry) (commitTimestamp int64, err error) {
	serializedEntriesBytes := SerializeEntries(entries)
	_, responseBody, err := this.Request("PUT", map[string]string{}, bytes.NewReader(serializedEntriesBytes))

	if err != nil {
		return
	}

	responseObject := PutPostResponse{}

	err = json.Unmarshal(responseBody, &responseObject)
	if err != nil {
		return
	}

	return responseObject.CommitTimestamp, nil
}

func (this *Client) PostOrCreate(entries []Entry) (commitTimestamp int64, err error) {
	serializedEntriesBytes := SerializeEntries(entries)
	_, responseBody, err := this.Request("POST", map[string]string{"create": "true"}, bytes.NewReader(serializedEntriesBytes))

	if err != nil {
		return
	}

	responseObject := PutPostResponse{}

	err = json.Unmarshal(responseBody, &responseObject)
	if err != nil {
		return
	}

	return responseObject.CommitTimestamp, nil
}

// Sends a DELETE request for this datastore
func (this *Client) Delete() (err error) {
	_, _, err = this.Request("DELETE", map[string]string{}, nil)
	return
}

// Sends an HTTP request to the datastore with the given method, arguments and body
func (this *Client) Request(method string, queryArgs map[string]string, requestBody io.Reader) (response *http.Response, responseBody []byte, err error) {
	url := this.BuildRequestURL(queryArgs)

	request, err := http.NewRequest(method, url, requestBody)

	if err != nil {
		return
	}

	client := &http.Client{}

	response, err = client.Do(request)
	if err != nil {
		return
	}

	responseBody, err = ReadEntireStream(response.Body)
	if err != nil {
		return
	}

	if response.StatusCode != 200 {
		return response, responseBody, errors.New("Response status: " + response.Status)
	}

	return
}
//type webSocketClientReadNextFunc func() ([]Entry, error)

func (this *Client) OpenWebSocket(updatedAfter int64)  (func() ([]Entry, error), error) {
	dialer := &websocket.Dialer{}
	queryArgs := map[string]string{}

	if updatedAfter > 0 {
		queryArgs["updatedAfter"] = fmt.Sprintf("%d", updatedAfter)
	}

	requestURL := this.BuildRequestURL(queryArgs)
	requestURL = "ws://" + requestURL[7:]
	conn, _, err := dialer.Dial(requestURL, nil)

	if err != nil {
		return nil, err
	}

	return func() ([]Entry, error) {
		for {
			messageType, reader, err := conn.NextReader()
			if err != nil {
				return nil , err
			}

			if messageType == websocket.BinaryMessage {
				entryStreamBytes, err := ReadEntireStream(reader)

				if err != nil {
					return nil , err
				}

				return DeserializeEntryStreamBytes(entryStreamBytes)
			}
		}
	}, nil
}

func (this *Client) BuildRequestURL(queryArgs map[string]string) string {
	queryComponents := []string{}

	for k, v := range queryArgs {
		if v != "" {
			queryComponents = append(queryComponents, fmt.Sprintf("%s=%s", k, v))
		}
	}

	if this.accessKey != "" {
		queryComponents = append(queryComponents, fmt.Sprintf("accessKey=%s", this.accessKey))
	}

	queryString := "?" + strings.Join(queryComponents, "&")

	if len(queryString) > 1 {
		return this.hostURL + "/datastore/" + this.datastoreName + queryString
	} else {
		return this.hostURL + "/datastore/" + this.datastoreName
	}
}
