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

**Response**:

The requested data, contained in the response body in the native binary format (described in detail in an appendix section below).

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

**Arguments**:

* `accessKey` (string, optional): Access key.

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

The related configuration datastore `<DatastoreName>.config` is not deleted automatically. A `DELETE` operation needs to be issued to it separately.