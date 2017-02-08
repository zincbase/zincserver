# ZincServer

ZincServer is a high-performance server back-end for [ZincDB](https://github.com/zincbase/zincdb) and beyond. Written in Go.

## Status

The software is mostly feature complete, but still at an alpha level of stability. Further automated and real-world testing would be required before it is ready for production use. Please report any crash, unexpected behavior, security weakness or missing feature at the [issue tracker](https://github.com/zincbase/zincserver/issues).

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

[MIT](https://github.com/zincbase/zincserver/blob/master/LICENSE)
