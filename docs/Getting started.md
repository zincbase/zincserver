# Getting started

## Building the server

Make sure [Git](https://git-scm.com/downloads) and [Go](https://golang.org/dl/) are installed.

Once `GOPATH` is correctly configured, run:

```
go get -u -v github.com/zincbase/zincserver
```

A `zincserver.exe` (Windows) or `zincserver` (Linux) executable should be created in your `$GOPATH/bin` directory.

(pre-compiled binaries may become available in the future as well)

## Launching the server

Starting an insecure (HTTP) listener on port `8000` with `"./datastores"` as a storage directory:

```
./zincserver start -insecurePort 8000 -storagePath "./datastores"
```

Starting a secure (HTTPS) listener on port `8001` with `"./datastores"` as a storage directory and HTTP2 enabled:

```
./zincserver start -securePort 8001 -storagePath "./datastores" -keyFile "myKey.pem" -certFile "myCert.pem" -enableHTTP2
```

_(note it is possible to run both an HTTP and an HTTPS listener concurrently)_

Run `zincserver start -help` for more startup options.

## Initializing global configuration and getting a master key.

When the server is launched, it creates a special `.config` datastore at the specified storage path, if one doesn't exist already. The global configuration datastore is initialized with a boilerplate default configuration and a securely generated master key, which is printed to the console. The master key grants full access to all datastores and is the only access key permitted to view or modify a configuration datastore (both global and dedicated ones).

## Configuring the server

ZincServer allows two levels of configuration. Global and datastore-specific. Global configuration is stored at the `.config` datastore, and datastore-specific settings can be specified in a datastore named `<DatastoreName>.config`. Datastore-specific configuration inherits from the global one and would override any existing global setting when a value with identical key is set.

ZincServer configuration is fully live and can be freely modified while the server is running and data is read or written to it, with changes taking effect instantly and no need for any restarts. The web-based editor tool can be used to conveniently create and manage datastores, and datastore configurations, set up access keys and profiles. Since configuration datastores aren't different from regular ones, they can be created and modified using the REST API, or any higher level API (they can be cloned and modified as normal ZincDB databases).

## Setting up the editor

The editor is a web-based application that can view and edit any ZincDB compatible datastore, including ZincServer configuration datastores.

1. Make sure you have the latest [Node.js](https://nodejs.org/en/) installed.
2. Download the [ZincDB repository](https://github.com/zincbase/zincdb) by selecting `Clone or Download` -> `Download ZIP` at the main repository page (or alternatively clone the repository by running `git clone https://github.com/zincbase/zincdb.git`).
3. Unzip if needed.
4. Run `npm install`, `npm run build`, `npm run devserver` at the repository's root directory.
5. Open a web browser at `http://localhost:8888/editor`.
6. Once the editor opens, fill the global configuration datastore URI (`http://localhost:[ZincServerPort]/datastore/.config`) and master key as access key and press enter.

## Setting up access keys

ZincServer's access control is based on _access keys_, which must be 32 character lowercase hexadecimal strings. An access key is associated with an _access profile_ that describes what permissions and limits are allowed for that key. A single access profile can be shared across multiple access keys.

The default configuration contains two boilerplate access profiles: `Reader` and `ReaderWriter`. To add an access key `912ec803b2ce49e4a541068d495ab570` associated with the profile `Reader` and specific to the datastore `MyDatastore` create a new datastore `MyDatastore.config`, add the path:

```
["datastore","accessKeyHash","a84825308039ffcc6ea3cdb0022776079651bd00"]
```

and give it the value `"Reader"` (note the string used in the path is the hexadecimal encoded SHA1 hash of the the target access key as plain string).

## Modifying and creating access profiles, configuring limits, quotas and misc settings

Please continue to the [configuration reference](https://github.com/zincbase/zincserver/blob/master/docs/Configuration%20reference.md) for more details.

## Low level REST API

Please continue to the [REST API reference](https://github.com/zincbase/zincserver/blob/master/docs/REST%20API%20reference.md).
