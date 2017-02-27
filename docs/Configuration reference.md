# ZincServer configuration reference

_(For more general information regarding ZincServer's configuration structure, and instructions on setting up a server instance see the [getting started guide](https://github.com/zincbase/zincserver/blob/master/docs/Getting%20started.md))_.

Configuration entry keys are node paths of the form `["<identifier>", "<identifier>", "<identifier>",..]`*. Values are JSON encoded strings and can have any one of the types `string`, `number` or `boolean` (objects and arrays are not supported by the server at the time being). This document also uses more specific types like `integer` or `float` for reference purposes.

There are two layers of configuration: Global configuration, contained in the `.config` datastore, and particular, contained in `<datastoreName>.config` datastore, which can be created for any datastore except the configuration datastores themselves. Both datastore-specific and global settings can be combined, where the datastore-specific settings always take precedence, if exists.

_The actual underlying raw encoding is a JSON string of the form `"['<identifier>']['<identifier>']['<identifier>']...".` The array-like format used here is similar to the one shown by the editor and used in the ZincDB JavaScript API._

## Server settings

Server settings affect the entire server instance:

* `["server","masterKeyHash"]` (string): Lowercase hex encoded SHA-1 hash of the master key. This must be 40 characters long (representing 160 bits).

## Access profile definitions

Access profiles are sets of configuration entries that specify permissions and quotas for any access key that is set to point to them. Every HTTP method (e.g. `GET`, `POST`, `PUT` etc.) is configured separately.

* `["accessProfile",<AccessProfileName>,"method",<"GET" | "POST" | "PUT" | "DELETE" | "WebSocket">,"allowed"]` (boolean): Allow requests of the method type specified in the path.
* `["accessProfile",<AccessProfileName>,"method",<"GET" | "PUT" | "POST" | "DELETE" | "WebSocket">,"limit","requests","count"]` (integer): Maximum requests allowed per time interval per each individual origin IP. Note that that since the limit is per origin, multiple clients can connect from different IPs with a shared access key, such that the limit would be separately applied to each group of clients sharing an IP. For the `WebSocket` method, a request is counted as the initiation of a WebSocket. Individual WebSocket messages are not counted as requests.
* `["accessProfile",<AccessProfileName>,"method",<"GET" | "PUT" | "POST" | "DELETE"| "WebSocket">,"limit","requests","interval"]` (integer): Interval (milliseconds) for corresponding maximum requests limit.
* `["accessProfile",<AccessProfileName>,"method",<"GET" | "PUT" | "POST" | "DELETE"| "WebSocket">,"param",<ParamName>,"allowed"]` (boolean): Allow or disallow the HTTP request parameter specified in the path (`<ParamName>`). By default all parameters except `auth` are disallowed for all methods unless explicitly enabled.

## Datastore settings

Datastore settings are settings applied to each datastore (or globally to all datastores if specified in `.config`).

* `["datastore","accessKeyHash",<AccessKeyHash>]` (string): The name of the access profile to associate with the access key hash specified in the path. The `<AccessKeyHash>` should be the lowercase hexadecimal encoding of the SHA1 hash of the target access key interpreted as a plain UTF-8 string (the hex characters should not be converted to binary before hashing).
* `["datastore","limit","maxSize"]` (integer): Maximum allowed size of the datastore file. Note this limit would not account for redundant entries that may be removed during compaction, so it is recommended to set a limit several times greater than the target one to account for them.
* `["datastore","flush","enabled"]` (boolean): Enable datastore file flushing (or "sync") operations. Having this option disabled would leave the management of flushing operations the operating system (if the file system uses write-behind, this may mean that writes may takes an arbitrarily long amount of time to be persisted to physical media, though that may significantly improve write performance).
* `["datastore","flush","maxDelay"]` (integer): Maximum time interval, in milliseconds, between the time the datastore file is written to until it is persisted to physical media.
* `["datastore","compaction","enabled"]` (boolean): Enable automated datastore file compaction.
* `["datastore","compaction","minSize"]` (integer): Minimal datastore file size threshold for compaction checks to be performed.
* `["datastore","compaction","minUnusedSizeRatio"]` (float): Minimal ratio between the unused (redundant) and total datastore file size that would cause a compaction to be performed.
* `["datastore","compaction","minGrowthRatio"]` (float): Minimal ratio between the current datastore size to its size when the previous compaction check was performed, such that subsequent compaction check is triggered.
* `["datastore","CORS","origin",<OriginURI | "*">,"allowed"]` (boolean): Allow cross-origin requests from the origin specified in the path. Specifying origin URI as `"*"` would apply to all origins.
