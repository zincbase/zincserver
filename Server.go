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
	"strings"
	"sync"
)

type ServerStartupOptions struct {
	InsecurePort                 int
	InsecureListenerLoopbackOnly bool
	SecurePort                   int
	SecureListenerLoopbackOnly   bool
	CertFile                     string
	KeyFile                      string
	EnableHTTP2                  bool
	StoragePath                  string
	LogLevel                     int
	NoAutoMasterKey              bool
	Profile                      bool
}

func DefaultServerStartupOptions() *ServerStartupOptions {
	return &ServerStartupOptions{
		InsecurePort:                 0,
		InsecureListenerLoopbackOnly: false,
		SecurePort:                   0,
		SecureListenerLoopbackOnly:   false,
		CertFile:                     "cert.pem",
		KeyFile:                      "key.pem",
		EnableHTTP2:                  false,
		StoragePath:                  "",
		LogLevel:                     1,
		NoAutoMasterKey:              false,
		Profile:                      false,
	}
}

type Server struct {
	startupOptions *ServerStartupOptions

	datastores       map[string]*DatastoreOperations
	datastoreMapLock *sync.Mutex

	insecureListener *ServerListener
	secureListener   *ServerListener

	runningStateWaitGroup *sync.WaitGroup
	bannedIPs             map[string]bool
	rateLimiter           *RateLimiter
}

func NewServer(startupOptions *ServerStartupOptions) *Server {
	return &Server{
		startupOptions:        startupOptions,
		datastores:            make(map[string]*DatastoreOperations),
		datastoreMapLock:      &sync.Mutex{},
		runningStateWaitGroup: &sync.WaitGroup{},
		bannedIPs:             make(map[string]bool),
		rateLimiter:           NewRateLimiter(),
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

	// Load global configuration datastore
	globalConfigDatastore := this.GetDatastoreOperations(".config")
	_, err = globalConfigDatastore.LoadIfNeeded(false)

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

			// Initialize default configuration datastore
			defaultConfigBytes := DefaultServerConfig(newMasterKey)

			timestamp := MonoUnixTimeMicro()
			err = ValidateAndPrepareTransaction(defaultConfigBytes, timestamp, 0)
			if err != nil {
				panic(err)
			}

			err = globalConfigDatastore.Rewrite(defaultConfigBytes, timestamp)
			if err != nil {
				panic(err)
			}

		default:
			panic(err)
		}
	}

	if globalConfigDatastore.State == nil {
		panic(errors.New("Failed loading or creating global configuration datastore"))
	}

	if this.startupOptions.SecurePort > 0 {
		cer, err := tls.LoadX509KeyPair(this.startupOptions.CertFile, this.startupOptions.KeyFile)
		if err != nil {
			panic(err)
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
			panic(err)
		}

		this.secureListener = NewServerListener(this, tlsListener, "https", this.startupOptions.SecureListenerLoopbackOnly)

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
		this.insecureListener = NewServerListener(this, tcpListener, "http", this.startupOptions.InsecureListenerLoopbackOnly)

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
		datastore.Close()
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
		datastoreOperations = NewDatastoreOperations(datastoreName, this, IsConfigDatastoreName(datastoreName))

		// Create a new map object
		newMap := make(map[string]*DatastoreOperations)

		// Clone the existing map into it
		for key, value := range this.datastores {
			newMap[key] = value
		}

		// Set the new entry in the map
		newMap[datastoreOperations.Name] = datastoreOperations

		// Atomically replace the old map with the new map
		this.datastores = newMap

		// Unlock
		this.datastoreMapLock.Unlock()
	}

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Configuration operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Gets an immutable configuration snapshot for a particular datastore
func (this *Server) GetConfigSnapshot(datastoreName string) (*DatastoreConfigSnapshot, error) {
	globalConfigDatastoreState := this.GetDatastoreOperations(".config").State

	var globalConfig *VarMap

	if globalConfigDatastoreState == nil {
		globalConfig = nil
	} else {
		globalConfig = globalConfigDatastoreState.DataCache
	}

	// If the target datastore is itself a configuration datastore
	if IsConfigDatastoreName(datastoreName) {
		// Only include the global config in the snapshot
		return NewDatastoreConfigSnapshot(globalConfig, nil), nil
	} else { // Otherwise
		// Include its dedicated config as well, if exists
		dedicatedConfigDatastoreState, err := this.GetDatastoreOperations(datastoreName + ".config").LoadIfNeeded(false)
		var dedicatedConfig *VarMap = nil

		if err == nil {
			dedicatedConfig = dedicatedConfigDatastoreState.DataCache
		} else {
			switch err.(type) {
			// If no dedicated config file was found, set the dedicated config map to nil
			case *os.PathError:
				dedicatedConfig = nil
			default:
				return nil, err
			}
		}

		return NewDatastoreConfigSnapshot(globalConfig, dedicatedConfig), nil
	}
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

func IsConfigDatastoreName(datastoreName string) bool {
	return strings.HasSuffix(datastoreName, ".config")
}
