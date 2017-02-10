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
	datastoreOperationEntries map[string]*DatastoreOperationsEntry
	operationsMapLock         *sync.Mutex

	insecureListener *ServerListener
	secureListener   *ServerListener
	startupOptions   *ServerStartupOptions

	runningStateWaitGroup *sync.WaitGroup
	bannedIPs             map[string]bool

	globalConfigDatastoreOperations *DatastoreOperationsEntry
}

type ServerStartupOptions struct {
	InsecurePort    int64
	SecurePort      int64
	EnableHTTP2     bool
	CertFile        string
	KeyFile         string
	StoragePath     string
	LogLevel        int64
	NoAutoMasterKey bool
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
	this.globalConfigDatastoreOperations = this.GetDatastoreOperations(".config")

	err = this.globalConfigDatastoreOperations.LoadIfNeeded()
	if err != nil {
		switch err.(type) {
		case *os.PathError:
			newMasterKey := ""

			if !this.startupOptions.NoAutoMasterKey {
				newMasterKeyBytes := make([]byte, 16)
				rand.Read(newMasterKeyBytes)
				newMasterKey = hex.EncodeToString(newMasterKeyBytes)
			}

			fmt.Println("No configuration datastore found.")
			fmt.Println("Creating default one with master key '" + newMasterKey + "'.")
			fmt.Println("")

			_, err = this.globalConfigDatastoreOperations.Rewrite(DefaultServerConfig(newMasterKey))
			if err != nil {
				panic(err)
			}

			err = this.globalConfigDatastoreOperations.LoadIfNeeded()
			if err != nil {
				panic(err)
			}

		default:
			panic(err)
		}
	}

	if this.globalConfigDatastoreOperations.dataCache == nil {
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
		secureListener, err := tls.Listen("tcp", listenerAddress, &tlsConfig)
		if err != nil {
			panic(err.Error())
		}
		this.secureListener = NewServerListener(this, secureListener, "https")

		go http.Serve(this.secureListener, NewServerHandler(this))
		this.runningStateWaitGroup.Add(1)
		log.Printf("Started secure server at port %d", this.startupOptions.SecurePort)
	}

	if this.startupOptions.InsecurePort > 0 {
		// Start HTTP Listener
		listenerAddress := fmt.Sprintf(":%d", this.startupOptions.InsecurePort)
		insecureListener, err := net.Listen("tcp", listenerAddress)
		if err != nil {
			panic(err)
		}
		this.insecureListener = NewServerListener(this, insecureListener, "http")

		go http.Serve(this.insecureListener, NewServerHandler(this))
		this.runningStateWaitGroup.Add(1)
		log.Printf("Started insecure server at port %d", this.startupOptions.InsecurePort)
	}
}

func (this *Server) Stop() {
	if this.insecureListener != nil {
		this.insecureListener.Close()
		this.insecureListener = nil
		this.runningStateWaitGroup.Done()
	}

	if this.secureListener != nil {
		this.secureListener.Close()
		this.secureListener = nil
		this.runningStateWaitGroup.Done()
	}
}

func (this *Server) Log(message string, logLevel int64) {
	if this.startupOptions.LogLevel >= logLevel {
		log.Println(message)
	}
}

func (this *Server) GetDatastoreOperations(datastoreName string) (datastoreOperations *DatastoreOperationsEntry) {
	// Create a new entry if it truly doesn't exist
	this.operationsMapLock.Lock()
	datastoreOperations = this.datastoreOperationEntries[datastoreName]

	if datastoreOperations == nil {
		datastoreOperations = NewDatastoreOperationsEntry(datastoreName, this)
		this.datastoreOperationEntries[datastoreName] = datastoreOperations
	}
	this.operationsMapLock.Unlock()

	if !datastoreOperations.IsConfig() {
		datastoreOperations.configDatastore = this.GetDatastoreOperations(datastoreName + ".config")
	}

	return
}

func (this *Server) GlobalConfig() *VarMap {
	globalConfig := this.globalConfigDatastoreOperations.dataCache

	if globalConfig != nil {
		return globalConfig
	} else {
		return NewEmptyVarMap()
	}
}

func NewServer(startupOptions *ServerStartupOptions) *Server {
	return &Server{
		datastoreOperationEntries: make(map[string]*DatastoreOperationsEntry),
		operationsMapLock:         &sync.Mutex{},
		startupOptions:            startupOptions,
		runningStateWaitGroup:     &sync.WaitGroup{},
	}
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
