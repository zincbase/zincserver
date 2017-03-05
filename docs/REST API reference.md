# ZincServer REST API Reference

## `GET`

Read the content of a datastore.

**Example**:

```
GET https://example.com:1337/datastore/MyDatastore?updatedAfter=1464852127534534&accessKey=3da541559918a808c2402bba5012f6c6
```

**Arguments**:

* `accessKey` (string, optional): An access key to provide credentials for the operation, if needed. If provided, must be 32 lowercase hexadecimal characters. Defaults to the empty string (`""`).
* `updatedAfter` (number, optional): Only include revisions after particular time. Value is a UNIX epoch microsecond timestamp, Defaults to `0`.
* `waitUntilNonempty` (boolean, optional): If no results are immediately available, the server would wait until at least one is available before responding. In combination with `updatedAfter`, it can be used to achieve the COMET pattern for near real-time synchronization. Defaults to `false`.

**Response**:

The requested data serialized in the [native binary format](https://github.com/zincbase/zincserver/blob/master/docs/Binary%20format%20specification.md).

## `GET` (WebSocket upgrade)

Create a WebSocket to fetch existing data, and receive any future modifications of the datastore in real-time.

**Example**:

```
GET (WebSocket upgrade) wss://example.com:1337/datastore/MyDatastore?updatedAfter=1464852127534534&accessKey=3da541559918a808c2402bba5012f6c6
```

**Arguments**:

Similar to regular `GET`. `waitUntilNonempty` argument is not applicable and would be ignored.

**Response**:

Opens a WebSocket. The WebSocket is unidirectional and would send a stream of messages similar in form to GET response bodies. The messages would include both past and future revisions that match the given query.

**Notes**:

If the client attempts to send a binary or text message to the server, it would be immediately disconnected.

## `POST`

Append new revisions to the datastore.

**Arguments**:

* `accessKey` (string, optional): Access key.
* `create` (boolean, optional): Create a new datastore, if it doesn't currently exist.

Request body should contain a non-empty stream of [serialized revision entries](https://github.com/zincbase/zincserver/blob/master/docs/Binary%20format%20specification.md).

**Response**:

A JSON encoded object of the form:

```json
{
	"commitTimestamp": ......
}
```
Where `commitTimestamp` is a millisecond UNIX epoch encoding of the timestamp representing the transaction

**Example**:

```
POST https://example.com:1337/datastore/MyDatastore&accessKey=3da541559918a808c2402bba5012f6c6
```

**Notes**:

If the given request body is empty, the request would be rejected with a 400 (Bad Request) error. This includes the case where the datastore file doesn't exist and the `create` flag was set to `true`. The reasoning for the strict handling of this case is that since the server cannot provide a valid commit timestamp, the client should never rely on any value that would have been given for future `GET` requests.

## `PUT`

Rewrite the datastore with the given revisions. If the datastore doesn't exist, it will be created.

**Arguments**:

* `accessKey` (string, optional): Access key.

Request body should a (possibly empty) stream of [serialized revision entries](https://github.com/zincbase/zincserver/blob/master/docs/Binary%20format%20specification.md).

**Response**:

Similar to `POST`.

**Example**:

```
PUT https://example.com:1337/datastore/MyDatastore&accessKey=3da541559918a808c2402bba5012f6c6
```

## `DELETE`

Destroys the entire datastore. All data is permanently deleted.

**Arguments**:

* `accessKey` (string, optional): Access key.

**Example**:

```
DELETE https://example.com:1337/datastore/MyDatastore&accessKey=3da541559918a808c2402bba5012f6c6
```

**Notes**:

The related configuration datastore `<DatastoreName>.config` is not automatically destroyed. A separate `DELETE` operation can to be issued to it, if desired.
