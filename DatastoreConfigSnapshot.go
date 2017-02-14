package main

import ()

// An object storing the configuration state for a particular datastore
type DatastoreConfigSnapshot struct {
	datastoreConfig *VarMap
	globalConfig    *VarMap
}

// A constructor
func NewDatastoreConfigSnapshot(datastoreConfig *VarMap, globalConfig *VarMap) *DatastoreConfigSnapshot {
	return &DatastoreConfigSnapshot{
		datastoreConfig: datastoreConfig,
		globalConfig:    globalConfig,
	}
}

// Gets a string typed configuration value.
func (this *DatastoreConfigSnapshot) GetString(key string) (value string, err error) {
	// If a datastore-specific configuration is available
	if this.datastoreConfig != nil {
		// Lookup the value for the given key
		value, err = this.datastoreConfig.GetString(key)

		// If the key was found
		if err == nil {
			// Return its value
			return
		}
	}

	if this.globalConfig == nil {
		return
	}

	// Otherwise, look up the key in the global configuration and return its value if found
	return this.globalConfig.GetString(key)
}

// Gets a boolean typed configuration value.
func (this *DatastoreConfigSnapshot) GetBool(key string) (value bool, err error) {
	// If a datastore-specific configuration is available
	if this.datastoreConfig != nil {
		// Lookup the value for the given key
		value, err = this.datastoreConfig.GetBool(key)

		// If the key was found
		if err == nil {
			// Return its value
			return
		}
	}

	if this.globalConfig == nil {
		return
	}

	// Otherwise, look up the key in the global configuration and return its value if found
	return this.globalConfig.GetBool(key)
}

// Gets a 64-bit integer typed configuration value.
func (this *DatastoreConfigSnapshot) GetInt64(key string) (value int64, err error) {
	// If a datastore-specific configuration is available
	if this.datastoreConfig != nil {
		// Lookup the value for the given key
		value, err = this.datastoreConfig.GetInt64(key)

		// If the key was found
		if err == nil {
			// Return its value
			return
		}
	}

	if this.globalConfig == nil {
		return
	}

	// Otherwise, look up the key in the global configuration and return its value if found
	return this.globalConfig.GetInt64(key)
}

// Gets a 64-bit float typed configuration value.
func (this *DatastoreConfigSnapshot) GetFloat64(key string) (value float64, err error) {
	// If a datastore-specific configuration is available
	if this.datastoreConfig != nil {
		// Lookup the value for the given key
		value, err = this.datastoreConfig.GetFloat64(key)

		// If the key was found
		if err == nil {
			// Return its value
			return
		}
	}

	if this.globalConfig == nil {
		return
	}

	// Otherwise, look up the key in the global configuration and return its value if found
	return this.globalConfig.GetFloat64(key)
}
