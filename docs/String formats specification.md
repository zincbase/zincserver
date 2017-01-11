# String formats specification

### `"jsonObject"`

A standard JSON object of the form:

```json
{
	"key1": "value1",
	"key2": "value2",
	"key3": "value3",
	...
}
``` 

Note that it may contain duplicate keys. Make sure the JSON parser you're using supports them and automatically resolves them to the latest value. Also note this format doesn't include any timestamp and thus is not suitable for synchronization.

### `"jsonArray"`

A standard JSON array of the form:

```json
[
	{"timestamp": 1464852127534534, "key": "key1", "value": "value1"},
	{"timestamp": 1464852127534564, "key": "key1", "value": "value2"},
	{"timestamp": 1464852127534576, "key": "key1", "value": "value3"},
	...
]
``` 

If received from the server, the `timestamp` property would represent the entry commit time.

### `"jsonStream"`:

A non JSON-compliant stream of individual JSON objects, separated by line feeds:

```json
{"timestamp": 1464852127534534, "key": "key1", "value": "value1"}\n
{"timestamp": 1464852127534564, "key": "key1", "value": "value2"}\n
{"timestamp": 1464852127534576, "key": "key1", "value": "value3"}\n
...
``` 

### `"tabbedJson"`

A tab and line-feed delimited sequence of JSON values. Property keys are pre-defined and values are separated by tabs (`\t`). Individual entries are separated by single line feed (`\n`) characters:

```json
1464852127534534\t"key1"\t"value1"\n
1464852127534564\t"key2"\t"value2"\n
1464852127534564\t"key3"\t"value3"\n
...
```

### `"tabbedJsonShort"`

Same as `"tabbedJson"`, only timestamps are omitted. Intended mostly for testing basic POST and PUT requests.

```json
"key1"\t"value1"\n
"key2"\t"value2"\n
"key3"\t"value3"\n
...
``` 

### Some notes about the usage of these formattings

In `GET` requests, due to the overhead of conversion, using any formatting other then the raw binary one would result in a reduction of performance of about 50%-75%. It should mostly be used for local deployments or when performance is not critical.

In `POST` and `PUT` requests, only the `"tabbedJson"` and `"tabbedJsonShort"` formattings are currently allowed. This limitation is intentional as other formats require significantly more complex and memory expensive parsing.