# ZincServer REST API Reference

## `GET`

Read the content of a datastore.

**Example**:

```
GET https://mydomain.com/datastore/MyDatastore?updatedAfter=1464852127534534&accessKey=3da541559918a808c2402bba5012f6c6
```

**Arguments**:

* `accessKey` (string, optional): An access key to provide credentials for the operation, if needed. If provided, must be 32 lowercase hexadecimal characters. Defaults to the empty string (`""`).
* `updatedAfter` (number, optional): Only include revisions after particular time. Value is a UNIX epoch microsecond timestamp, Defaults to `0`.
* `waitUntilNonempty` (boolean, optional): If no results are immediately available, wait until some are available before responding. In combination with `updatedAfter`, it can be used to achieve the COMET pattern for near real-time synchronization. Defaults to `false`.
* `format` (string, optional): The format to use for the response. Either `raw` (default). `jsonObject`, `jsonArray`, `jsonStream`, `tabbedJson`, `tabbedJsonShort`. More detailed descriptions of these formats are included in an appendix section below.
* `compact` (boolean, optional): Compact revisions sets before they are sent. Currently mostly experimental.

**Response**:

The requested data, contained in the response body. Either in the native binary format or in the requested string formats (both are described in detail in appendix sections below).

**Notes**:

In string formatted responses: if the result set contains an revision with a binary or encrypted key or value, it would be encoded as a BASE64 JSON string. A UTF-8 typed key or value would be encoded as a JSON string as well. Since there would be no way to discriminate between different encodings, the client would need to have pre-programmed knowledge that these particular keys are associated with non-JSON content, there is currently no additional type metadata given when using string formatted requests. 

## `GET` (WebSocket upgrade)

Create a WebSocket to both fetch existing data, and receive real-time updates for future modifications of the datastore.

**Example**:

```
GET (WebSocket upgrade) wss://mydomain.com:1337/datastore/MyDatastore?updatedAfter=1464852127534534&accessKey=3da541559918a808c2402bba5012f6c6
```

**Arguments**:

Similar to regular `GET`. `waitUntilNonempty` argument is not applicable and would be ignored.

**Response**:

Opens a WebSocket. The WebSocket is unidirectional and would send a stream of messages similar in form to GET response bodies. The messages would include both past and future revisions that match the given query.

## `POST`

Commit new revisions to the datastore.

**Arguments**:

* `accessKey`(string, optional): Access key.

**Example (1)**:

```
POST https://mydomain.com:1337/datastore/MyDatastore&accessKey=3da541559918a808c2402bba5012f6c6
```
Request body containing a stream of binary revision entries:
```
[Header (32 bytes)][Key][Value][Header (32 bytes)][Key][Value][Header (32 bytes)][Key][Value]...
```

See the appendix section below for a detailed description of the binary format.

**Example (2)**:

```
POST https://mydomain.com:1337/datastore/MyDatastore&format=tabbedJsonShort&accessKey=3da541559918a808c2402bba5012f6c6
```
Request body (formatted as `tabbedJsonShort`) containing a stream of entry data consisting of string keys and values, separated by line break. 
```
"key1"\t"value1"\n
"key2"\t"value2"\n
"key3"\t"value3"\n
```

See the appendix section below for a detailed description of the `tabbedJsonShort` format.

**Arguments**:

* `accessKey` (string, optional): Access key.
* `format` (string, optional): The format to use for the response. Either `raw` or unspecified (default). `tabbedJson`, `tabbedJsonShort`. More detailed descriptions of these formats is in an appendix section below.

**Notes**:

If the `tabbedJson` formatting is used, The timestamps included in the transmitted revisions, are taken as update timestamps. If `tabbedJsonShort` is used, the update timestamps are set by the server and are identical to the commit timestamps.

## `PUT`

Similar to `POST`, only all datastore content is cleared before new data is written to it.

## `DELETE`

Destroys the entire datastore. All data is permanently deleted.

**Arguments**:

* `accessKey` (string, optional): Access key.

**Example**:

```
DELETE https://mydomain.com:1337/datastore/MyDatastore&accessKey=3da541559918a808c2402bba5012f6c6
```

**Notes**:

Configuration settings particular for the datastore are not currently deleted automatically from the `@config` datastore. This cleanup should be managed separately if desired. A future enhancement might add a parameter or a configuration setting to erase datastore-specific settings as well.