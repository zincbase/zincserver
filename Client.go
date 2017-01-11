package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"fmt"
	//"log"
	"strings"
)

type Client struct {
	hostURL string
	datastoreName string
	accessKey string
}

func (this *Client) Get(updatedAfter int64) (entries []Entry, err error) {
	_, responseBody, err := this.Request("GET", map[string]string{"updatedAfter": fmt.Sprintf("%d", updatedAfter)}, nil)
	if err != nil {
		return
	}

	entries, err = DeserializeEntryStreamBytes(responseBody)

	return
}

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

func (this *Client) Delete() (err error) {
	_, _, err = this.Request("DELETE", map[string]string{}, nil)
	return
}

func (this *Client) Request(method string, queryArgs map[string]string, requestBody io.Reader) (response *http.Response, responseBody []byte, err error) {
	queryComponents := []string{}
	
	for k, v := range queryArgs {
		if v != "" {
			queryComponents = append(queryComponents, fmt.Sprintf("%s=%s", k, v));
		}
	}

	if this.accessKey != "" {
		queryComponents = append(queryComponents, fmt.Sprintf("accessKey=%s", this.accessKey));
	}

	url := this.hostURL + "/datastore/" + this.datastoreName + "?" + strings.Join(queryComponents, "&")

	request, err := http.NewRequest(method, url, requestBody)

	if err != nil {
		return
	}

	client := &http.Client{}

	response, err = client.Do(request)
	if err != nil {
		return
	}

	memoryWriter := NewMemoryWriter()
	_, err = io.Copy(memoryWriter, response.Body)
	if err != nil {
		return
	}

	responseBody = memoryWriter.WrittenData()

	if response.StatusCode != 200 {
		return response, responseBody, errors.New("Response status: " + response.Status)
	}

	return
}

func NewClient(hostURL string, datastoreName string, accessKey string) *Client {
	return &Client{hostURL: hostURL, datastoreName: datastoreName, accessKey: accessKey}
}

type PutPostResponse struct {
	CommitTimestamp int64 `json:"commitTimestamp"`
}