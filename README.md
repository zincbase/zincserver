# ZincServer

ZincServer is a high-performance server back-end for [ZincDB](https://github.com/zincbase/zincdb), and beyond. Written in Go.

## Design

ZincServer is an on-disk, transactional, networked persistence provider designed to efficiently perform a highly reduced set of operations needed for reliable data storage and synchronization.

In some aspects it resembles a [message broker](https://en.wikipedia.org/wiki/Message_broker). However, it models the data as a key-value store (though doesn't actually allow to retrieve individual keys), and intended to provide assurance of data integrity and long-term availability. Its security and customization features also allow it to be fully open to the global internet, and accessed directly from web browsers, desktop or mobile applications, reducing the need for custom application servers.

## Status

The software is mostly feature complete, but still at an alpha level of stability. Test coverage is high, and there are randomized tests for all major operations. However, further real-world testing and feature validation is still required before it is truly ready for production use. Please report any crash, unexpected behavior, security weakness or missing feature at the [issue tracker](https://github.com/zincbase/zincserver/issues).

Note that the binary storage format used by the engine might still be subject to breaking modifications before it is finalized. Any data stored with an alpha version of this software might become unreadable to a future version.

## Platform support

Currently only tested on Linux and Windows. Might also work on other platform/architecture combinations supported by the Go runtime (including big endian).

## Documentation

* [Getting started](https://github.com/zincbase/zincserver/blob/master/docs/Getting%20started.md)
* [REST API reference](https://github.com/zincbase/zincserver/blob/master/docs/REST%20API%20reference.md)
* [Configuration reference](https://github.com/zincbase/zincserver/blob/master/docs/Configuration%20reference.md)
* [Technical overview](https://github.com/zincbase/zincserver/blob/master/docs/Technical%20overview.md)
* [Binary format specification](https://github.com/zincbase/zincserver/blob/master/docs/Binary%20format%20specification.md)

## License

[Apache License 2.0](https://github.com/zincbase/zincserver/blob/master/LICENSE)
