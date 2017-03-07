# Binary format specification

## Overview

The 'raw' or binary entry stream is the internal format used natively in datastore files, raw server responses and WebSocket message payloads.

It is structured in the following way:
```
[header][key][value] [header][key][value] [header][key][value] [header][key][value] ...
```
Where the header, totaling 320 bits (40 bytes), is of the form:

```
[Total entry size (64-bit unsigned integer)]
[Header version (16-bit unsigned integer)]
[Key size (16-bit unsigned integer)]
[Key encoding (8-bit unsigned integer)]
[Value encoding (8-bit unsigned integer)]
[Encryption method (8-bit unsigned integer)]
[Flags (8-bit unsigned integer)]
[Update time (unix epoch, microseconds, 64-bit unsigned integer)]
[Commit time (unix epoch, microseconds, 64-bit unsigned integer)]
[Header checksum (32-bit unsigned integer)]
[Payload checksum (32-bit unsigned integer)]
```

(All values are little-endian)

Keys and values are arbitrary byte sequences. Keys can be up to `2^16-1` (`65535`) bytes of length. Values can be up to `2^64 - 1` bytes (`~18.45 exabytes`) of length.

## Key/Value encoding format

May be of:

* `0`: Binary
* `1`: UTF-8
* `2`: JSON
* `3`: OmniJson
* `4..127`: (reserved for future use)
* `128..255`: (user-defined)

## Encryption method

May be one of:

* `0`: Unencrypted
* `1`: AES-128-CBC
* `2`: AES-128-GCM (planned)
* `3..127`: (reserved for future use)
* `128..255`: (user-defined)

The `AES-128-CBC` Encryption method, which is the only one currently supported, should be performed at the client side, before the data is transmitted to the server, by individually encrypting a key using CBC (all zero IV), and individually encrypting the value with a random 128-bit IV, and prepending the IV to the resulting encrypted value. This results in an entry layout of the form `Unencrypted headers||Encrypted Key||IV||Encrypted Value` (where `||` denotes concatenation). Note the IV is considered a part of the value.

The `AES-128-GCM` Encryption format is planned but has not been designed yet.

The encrypted key is not semantically secure - multiple encryptions of the same key would result in the same ciphertext. However, this property is essential to allow for operations like key lookup and compaction to be possible at the server side.

**Important note**:

End-to-end encryption is meant to _complement_ an existing layer of security at the transport layer, such as a TLS socket paired with a high quality certificate. Having keys and values encrypted by clients before transmission provides some additional level of _privacy_ to clients and _encryption at rest_ for servers. However, it doesn't provide a meaningful guarantee of _tamper resistance_. It should not be, by any means, considered secure on its own.

## Flags

The currently used flags are:

* Bit `0` set: Transaction end marker - Marks the end of a transaction.
* Bits `1..7`: <reserved>

## Checksums

* Header checksum: a 32 bit CRC32C (I.e. Castagnoli) checksum for all the header bytes in the range [0:32]. Note this doesn't include the checksum itself.

* Payload checksum: a 32 bit CRC32C (I.e. Castagnoli) checksum for all the rest of the bytes in the entry. Starting at byte 40. Note this doesn't include the checksum itself.
