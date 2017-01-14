# Technical overview

ZincServer is a _chronological keyed datastore_. It is a time-based, key-aware storage engine only capable of only a few rudimentary operations:

1. Read all revisions that occurred after time X (`GET` operation). 
2. Append a set of new revisions to the datastore (`POST` operation).
3. Rewrite the datastore with a set of new revisions (`PUT` operation).
4. Delete the datastore (`DELETE` operation).

Each datastore is persisted in a single, append-only file. Each revision (or more precisely "revision message") is a binary block of data consisting of a header, a key and a value. Revisions are stored sequentially in the file, strictly ordered by time.

A datastore file is internally structured like the following:
```
[32 byte primary header][0..65535 byte secondary header][key][value], [32 byte primary header][0..65535 byte secondary header][key][value], [32 byte primary header][0..65535 byte secondary header][key][value] ...
```

When the server is started, it spawns an HTTP(s) server and becomes available for requests. It does not initially process or load any datastore (aside from its own configuration).

When a datastore is first referenced in a request, it is loaded (as well as its own configuration, if exists), and a minimal chronological index of its revisions is generated and stored in memory (or alternatively: a cached one is loaded from disk). This index is a very simple sorted list of the form:
```
(timestamp, offset), (timestamp, offset), (timestamp, offset), (timestamp, offset), ... 
```

1. To serve a `GET` request, the index is searched using linear/binary search and the offset of the earliest matching revision is found. The file is then read as-is, directly from disk, starting at the resulting offset and streamed to the response in binary, or incrementally converted into a target string format (`?format=...`). Note that this data may contain some amount of duplicate revisions (depending on the frequency of compactions). These are managed at the receiving client by keeping only the latest revision of a particular key and ignoring earlier ones (an experimental `&compact=true` argument is also available, which would perform the compaction on the server side, but should not be generally used).

2. To serve a 'POST' request, a bulk of raw revision data is sent by the client in the request body. These revisions are processed as a single transaction: scanned and verified, and stamped with a commit timestamp (microsecond resolution), where the last one of them receives a 'transaction end' flag. They are then appended to the datastore file and added to the index.

3. A 'PUT' request is similar to 'POST' only the database is cleared before the new revisions are written.

4. To compact the datastore, the datastore file is read and scanned to create a hash table that maps a key to its latest revision, and then rewritten only to include the latest revisions of each key.

Note: in order to 'delete' a particular key, a revision with that key is added with a zero-length value. This revision is still stored in the datastore and kept throughout compactions. In order to permanently delete a key, either the database needs to be rewritten or a compaction should be run with a special 'purge' flag (not currently implemented) that would permanently remove any revisions with no values.

## Managing concurrency between readers, writers and compactions

The server serializes write and compaction operations, however, read operations can happen concurrently, including concurrently to writes, rewrites and compactions. This is achieved by guarantying all file updates to be non-destructive in nature, and releasing old resources only when they are not needed:

* Read operations are bounded by a predetermined range within the file at the time of the request, so partially written data is never encountered by the reader.
* Compactions and Rewrites always create new files, and through a series of careful rename and delete operations, allow for the old file to still remain accessible to existing readers, but the new file to be visible for newer readers and writers. Once all existing readers of an old file have finished, the old file is immediately released from the file system and deleted. This may happen for several generations concurrently.

## An eye to the future: scaling to multiple machines

The datastore already provides very high performance on a single machine, and scales well in multi-core hardware. Scaling to multiple machines, most likely using master-replicator configuration, is currently under investigation and has been taken into account during the design process.