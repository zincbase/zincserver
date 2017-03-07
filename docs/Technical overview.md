# Technical overview

ZincServer is a _chronological keyed datastore_. It contains a time-based, key-aware storage engine only capable of a few rudimentary operations:

1. Read all revisions that occurred after time X (`GET` operation).
2. Append a set of new revisions to the datastore (`POST` operation).
3. Rewrite the datastore with a set of revisions (`PUT` operation).
4. Destroy the datastore (`DELETE` operation).

Each datastore is persisted in a single, append-only file. Each revision (or more precisely _revision message_) is a binary block of data consisting of a header, a key and a value. Both the key and the value can be arbitrary binary sequences. Revisions are stored sequentially in the file, strictly ordered by commit time.

A datastore file is internally structured like the following:
```
[header][key][value] [header][key][value] [header][key][value] ...
```

When the server is started, it spawns an HTTP(S) server and becomes available for requests. It does not initially process or load any datastore (aside from its own global configuration datastore).

When a datastore is first referenced in a request, its file is opened, and its configuration, if exists, is loaded to memory. A minimal chronological index of its revisions is generated and stored in memory (or alternatively: a cached one is loaded from disk). This index, in essence, is a very simple sorted list of the form:
```
(timestamp, offset), (timestamp, offset), (timestamp, offset), (timestamp, offset), ...
```

1. To serve a `GET` request, the index is searched using linear/binary search and the offset of the earliest matching revision is found. The file is then read as-is, directly from disk, starting at the resulting offset and streamed directly into the HTTP response in its raw encoding. Note that this data may contain some amount of duplicate revisions (depending on the frequency of compactions). These can be managed by the receiving client by only keeping the latest revision for a particular key and ignoring earlier ones.

2. To serve a `POST` request, a bulk of serialized revision data is sent by the client in the request body. These revisions are processed as a single transaction: scanned, verified, individually checksummed and stamped with a commit timestamp (microsecond resolution), where the last one of them receives a 'transaction end' flag. They are then appended to the datastore file and added to the index.

3. A `PUT` request is similar to `POST` only the datastore is cleared before the new revisions are written.

4. To compact the datastore, the datastore file is read and scanned to create a hash table that maps a key to its latest revision, and then rewritten only to include the latest revisions of each key (to save on memory the actual implementation uses the SHA1 hash of each key in place of its actual bytes).

Note: in order to delete a particular key, a revision with that key is added with a zero-length value. This revision is still stored in the datastore and preserved throughout compactions. This is done intentionally to ensure the key deletion event would be synchronized with all clients. To permanently delete a key, either the datastore needs to be rewritten, or a compaction should be run with a special 'purge' flag (not currently implemented) that would permanently discard any revisions with no values.

## Managing concurrency between readers, writers and compactions

The server serializes write, rewrite and compaction operations, however, read operations can happen concurrently, including concurrently to writes, rewrites and compactions. This is achieved by guaranteeing all file updates to be non-destructive in nature, and releasing old resources only when they are not needed:

* Read operations are bounded by a predetermined range within the file at the time of the request, so partially written data is never encountered by the reader.
* Compactions and rewrites always create new files, and through a series of careful rename and delete operations, allow for the old file to still remain accessible to existing readers, but the new file to be visible for newer readers and writers. Once all existing readers of an old file have completed, the old file is immediately released from the file system and deleted. This may happen for several generations concurrently.

## An eye to the future: scaling to multiple machines

The custom engine already provides very high performance on a single machine, and scales well in multi-core hardware. Scaling to multiple machines, most likely using master-replicator configuration, is currently under investigation and has been taken into account during the design process.
