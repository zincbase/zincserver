package main

import (
	"encoding/hex"
	"net/http"
)

type ServerTestContext struct {
	startupOptions *ServerStartupOptions
	server         *Server
	hostURL        string
}

func NewServerTestContext() *ServerTestContext {
	startupOptions := DefaultServerStartupOptions()
	startupOptions.InsecurePort = 12345
	startupOptions.StoragePath = "./tests_temp/"
	startupOptions.NoAutoMasterKey = true

	return &ServerTestContext{
		startupOptions: startupOptions,
		server:         NewServer(startupOptions),
		hostURL:        "http://localhost:12345",
	}
}

func (this *ServerTestContext) Start() {
	this.server.Start()
}

func (this *ServerTestContext) Stop() {
	this.server.Stop()

	// Ensure KeepAlive http connections are closed
	closer := http.DefaultTransport.(interface {
		CloseIdleConnections()
	})
	closer.CloseIdleConnections()

	// Destroy all datastores
	for _, datastore := range this.server.datastores {
		datastore.Destroy()
	}
}

func (this *ServerTestContext) GetClient(datastoreName string, accessKey string) *Client {
	return NewClient(this.hostURL, datastoreName, accessKey)
}

func (this *ServerTestContext) GetClientForRandomDatastore(accessKey string) *Client {
	return this.GetClient(RandomWordString(12), accessKey)
}

func (this *ServerTestContext) GetConfigClient(datastoreName string, accessKey string) *Client {
	return NewClient(this.hostURL, datastoreName+".config", accessKey)
}

func (this *ServerTestContext) GetGlobalConfigClient(accessKey string) *Client {
	return NewClient(this.hostURL, ".config", accessKey)
}

func (this *ServerTestContext) GetRandomEntries(maxCount int, maxKeySize int, maxValueSize int) []Entry {
	return GenerateRandomEntries(RandomIntInRange(1, maxCount), maxKeySize, maxValueSize, "randomBinaryEntry")
}

func (this *ServerTestContext) GetRandomAccessKey() (key string, keyHash string) {
	key = hex.EncodeToString(RandomBytes(16))
	keyHash = SHA1ToHex([]byte(key))

	return
}

func (this *ServerTestContext) GetTestEntries() []Entry {
	return []Entry{
		Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 1"), []byte(`"Key1"`), []byte(`"Value1"`)},
		Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 2"), []byte(`"Key2"`), []byte(`"Value2"`)},
		Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 3"), []byte(`"Key3"`), []byte(`"Value3"`)},
		Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 4"), []byte(`"Key4"`), []byte(`"Value4"`)},
		Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 5"), []byte(`"Key5"`), []byte(`"Value5"`)},
	}
}

func (this *ServerTestContext) PutGlobalSetting(key string, value string, accessKey string) (err error) {
	return this.PutDatastoreSetting("", key, value, accessKey)
}

func (this *ServerTestContext) PutGlobalSettings(key string, settingMap map[string]string, accessKey string) (err error) {
	return this.PutDatastoreSettings("", settingMap, accessKey)
}

func (this *ServerTestContext) PutDatastoreSetting(datastoreName string, key string, value string, accessKey string) (err error) {
	return this.PutDatastoreSettings(datastoreName, map[string]string{key: value}, accessKey)
}

func (this *ServerTestContext) PutDatastoreSettings(datastoreName string, settingMap map[string]string, accessKey string) (err error) {
	settingEntries := []Entry{}

	for key, value := range settingMap {
		settingEntries = append(settingEntries, Entry{
			PrimaryHeader:        &EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON},
			SecondaryHeaderBytes: nil,
			Key:                  []byte(key),
			Value:                []byte(value),
		})
	}

	configClient := this.GetConfigClient(datastoreName, accessKey)
	_, err = configClient.PostOrPut(settingEntries)

	return
}
