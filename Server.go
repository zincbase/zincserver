package main

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
)

type Server struct {
	datastores       map[string]*DatastoreOperations
	datastoreMapLock *sync.Mutex

	insecureListener *ServerListener
	secureListener   *ServerListener
	startupOptions   *ServerStartupOptions

	runningStateWaitGroup *sync.WaitGroup
	bannedIPs             map[string]bool
}

type ServerStartupOptions struct {
	InsecurePort    int
	SecurePort      int
	EnableHTTP2     bool
	CertFile        string
	KeyFile         string
	StoragePath     string
	LogLevel        int
	NoAutoMasterKey bool
}

func NewServer(startupOptions *ServerStartupOptions) *Server {
	return &Server{
		datastores:            make(map[string]*DatastoreOperations),
		datastoreMapLock:      &sync.Mutex{},
		startupOptions:        startupOptions,
		runningStateWaitGroup: &sync.WaitGroup{},
	}
}

func (this *Server) Start() {
	if this.startupOptions.StoragePath == "" {
		panic("No storage path specified.")
	}

	storagePathExists, err := DirectoryExists(this.startupOptions.StoragePath)
	if err != nil {
		panic(err)
	} else if storagePathExists == false {
		panic("The specified storage path '" + this.startupOptions.StoragePath + "' does not exist.")
	}

	// Load configuration datastore
	err = this.GlobalConfigDatastore().LoadIfNeeded()
	if err != nil {
		switch err.(type) {
		case *os.PathError:
			newMasterKey := ""

			if !this.startupOptions.NoAutoMasterKey {
				newMasterKeyBytes := make([]byte, 16)
				rand.Read(newMasterKeyBytes)
				newMasterKey = hex.EncodeToString(newMasterKeyBytes)
			}

			this.Log(0, "No configuration datastore found.")
			this.Log(0, "Creating default one with master key '"+newMasterKey+"'.")
			this.Log(0, "")

			_, err = this.GlobalConfigDatastore().Rewrite(DefaultServerConfig(newMasterKey))
			if err != nil {
				panic(err)
			}

			err = this.GlobalConfigDatastore().LoadIfNeeded()
			if err != nil {
				panic(err)
			}

		default:
			panic(err)
		}
	}

	if this.GlobalConfigDatastore().dataCache == nil {
		panic(errors.New("Failed loading or creating global configuration datastore"))
	}
	//

	if this.startupOptions.SecurePort > 0 {
		cer, err := tls.LoadX509KeyPair(this.startupOptions.CertFile, this.startupOptions.KeyFile)
		if err != nil {
			panic(err.Error())
		}

		tlsConfig := tls.Config{
			Certificates: []tls.Certificate{cer},
		}

		if this.startupOptions.EnableHTTP2 {
			tlsConfig.NextProtos = []string{"h2"}
		}

		listenerAddress := fmt.Sprintf(":%d", this.startupOptions.SecurePort)
		tlsListener, err := tls.Listen("tcp", listenerAddress, &tlsConfig)
		if err != nil {
			panic(err.Error())
		}
		this.secureListener = NewServerListener(this, tlsListener, "https")

		this.runningStateWaitGroup.Add(1)
		go func() {
			http.Serve(this.secureListener, NewServerHandler(this))
			this.runningStateWaitGroup.Done()
			this.Logf(0, "Secure listener at port %d has been closed", this.startupOptions.SecurePort)
		}()

		this.Logf(0, "Started secure listener at port %d", this.startupOptions.SecurePort)
	}

	if this.startupOptions.InsecurePort > 0 {
		// Start HTTP Listener
		listenerAddress := fmt.Sprintf(":%d", this.startupOptions.InsecurePort)
		tcpListener, err := net.Listen("tcp", listenerAddress)
		if err != nil {
			panic(err)
		}
		this.insecureListener = NewServerListener(this, tcpListener, "http")

		this.runningStateWaitGroup.Add(1)
		go func() {
			http.Serve(this.insecureListener, NewServerHandler(this))
			this.runningStateWaitGroup.Done()
			Logf("Insecure listener at port %d has been closed", this.startupOptions.InsecurePort)
		}()

		this.Logf(0, "Started insecure listener at port %d", this.startupOptions.InsecurePort)
	}
}

func (this *Server) Stop() {
	if this.insecureListener != nil {
		this.insecureListener.Close()
		this.insecureListener = nil
	}

	if this.secureListener != nil {
		this.secureListener.Close()
		this.secureListener = nil
	}
	this.runningStateWaitGroup.Wait()

	for _, datastore := range this.datastores {
		datastore.Release()
	}
}

func (this *Server) Log(logLevel int, values ...interface{}) {
	if this.startupOptions.LogLevel >= logLevel {
		log.Println(values...)
	}
}

func (this *Server) Logf(logLevel int, format string, values ...interface{}) {
	if this.startupOptions.LogLevel >= logLevel {
		log.Printf(format, values...)
	}
}

func (this *Server) GetDatastoreOperations(datastoreName string) (datastoreOperations *DatastoreOperations) {
	// Get the map entry for the datastore
	datastoreOperations = this.datastores[datastoreName]

	// If no entry exists
	if datastoreOperations == nil {
		// Lock
		this.datastoreMapLock.Lock()

		// Check again to ensure no data race occurred
		datastoreOperations = this.datastores[datastoreName]

		// If an operations object was found this time
		if datastoreOperations != nil {
			// Return it
			return
		}

		// Create a new operations object for the datastore
		datastoreOperations = NewDatastoreOperations(datastoreName, this)

		// Add it in the map
		this.PutDatastoreOperations(datastoreOperations)

		// Unlock
		this.datastoreMapLock.Unlock()
	}

	return
}

func (this *Server) PutDatastoreOperations(datastoreOperations *DatastoreOperations) {
	// Clone the map
	newMap := make(map[string]*DatastoreOperations)

	for key, value := range this.datastores {
		newMap[key] = value
	}

	// Set the new entry in the map
	newMap[datastoreOperations.name] = datastoreOperations

	// Replace the old map with the new map
	this.datastores = newMap
}

func (this *Server) GlobalConfigDatastore() *DatastoreOperations {
	return this.GetDatastoreOperations(".config")
}

func (this *Server) GlobalConfig() *VarMap {
	return this.GlobalConfigDatastore().dataCache
}

func DefaultServerConfig(masterKey string) []byte {
	var masterKeyHashHex string

	if len(masterKey) > 0 {
		masterKeyHashHex = SHA1ToHex([]byte(masterKey))
	} else {
		masterKeyHashHex = ""
	}

	defaultConfigStringEntries := []JsonEntry{
		JsonEntry{`"['server']['masterKeyHash']"`, `"` + masterKeyHashHex + `"`},

		JsonEntry{`"['server']['http']['loopbackOnly']"`, `false`},
		JsonEntry{`"['server']['https']['loopbackOnly']"`, `false`},

		JsonEntry{`"['datastore']['compaction']['enabled']"`, `true`},
		JsonEntry{`"['datastore']['compaction']['minSize']"`, `4096`},
		JsonEntry{`"['datastore']['compaction']['minGrowthRatio']"`, `2`},
		JsonEntry{`"['datastore']['compaction']['minUnusedSizeRatio']"`, `0.5`},

		JsonEntry{`"['datastore']['flush']['enabled']"`, `true`},
		JsonEntry{`"['datastore']['flush']['maxDelay']"`, `1000`},

		JsonEntry{`"['datastore']['limit']['maxSize']"`, `100000000`},

		JsonEntry{`"['datastore']['CORS']['origin']['*']['allowed']"`, `true`},

		JsonEntry{`"['accessProfile']['Reader']['method']['GET']['allowed']"`, `true`},
		JsonEntry{`"['accessProfile']['Reader']['method']['GET']['param']['updatedAfter']['allowed']"`, `true`},
		JsonEntry{`"['accessProfile']['Reader']['method']['GET']['param']['waitUntilNonempty']['allowed']"`, `true`},
		JsonEntry{`"['accessProfile']['Reader']['method']['GET']['limit']['requests']['interval']"`, `2000`},
		JsonEntry{`"['accessProfile']['Reader']['method']['GET']['limit']['requests']['count']"`, `10`},
		JsonEntry{`"['accessProfile']['Reader']['method']['WebSocket']['allowed']"`, `true`},
		JsonEntry{`"['accessProfile']['Reader']['method']['WebSocket']['param']['updatedAfter']['allowed']"`, `true`},
		JsonEntry{`"['accessProfile']['Reader']['method']['WebSocket']['limit']['parallelConnections']['max']"`, `8`},

		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['GET']['allowed']"`, `true`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['GET']['param']['updatedAfter']['allowed']"`, `true`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['GET']['param']['waitUntilNonempty']['allowed']"`, `true`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['GET']['limit']['requests']['interval']"`, `2000`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['GET']['limit']['requests']['count']"`, `10`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['WebSocket']['allowed']"`, `true`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['WebSocket']['param']['updatedAfter']['allowed']"`, `true`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['WebSocket']['limit']['parallelConnections']['max']"`, `8`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['POST']['allowed']"`, `true`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['POST']['limit']['requests']['interval']"`, `2000`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['POST']['limit']['requests']['count']"`, `10`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['PUT']['allowed']"`, `true`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['PUT']['limit']['requests']['interval']"`, `2000`},
		JsonEntry{`"['accessProfile']['ReaderWriter']['method']['PUT']['limit']['requests']['count']"`, `10`},
	}

	defaultConfigSerialized := SerializeJsonEntries(defaultConfigStringEntries)

	return defaultConfigSerialized
}

func DefaultServerStartupOptions() *ServerStartupOptions {
	return &ServerStartupOptions{
		InsecurePort:    0,
		SecurePort:      0,
		CertFile:        "cert.pem",
		KeyFile:         "key.pem",
		EnableHTTP2:     false,
		StoragePath:     "",
		LogLevel:        1,
		NoAutoMasterKey: false,
	}
}
