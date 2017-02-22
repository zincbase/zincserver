package main

import ()

// An object storing the configuration state for a particular datastore
type DatastoreConfigSnapshot struct {
	GlobalConfig    *VarMap
	DedicatedConfig *VarMap
}

// A constructor
func NewDatastoreConfigSnapshot(globalConfig *VarMap, dedicatedConfig *VarMap) *DatastoreConfigSnapshot {
	return &DatastoreConfigSnapshot{
		GlobalConfig:    globalConfig,
		DedicatedConfig: dedicatedConfig,
	}
}

// Gets a string typed configuration value.
func (this *DatastoreConfigSnapshot) GetString(key string) (value string, err error) {
	// If a datastore-specific configuration is available
	if this.DedicatedConfig != nil {
		// Lookup the value for the given key
		value, err = this.DedicatedConfig.GetString(key)

		// If the key was found
		if err == nil {
			// Return its value
			return
		}
	}

	if this.GlobalConfig == nil {
		return
	}

	// Otherwise, look up the key in the global configuration and return its value if found
	return this.GlobalConfig.GetString(key)
}

// Gets a string typed configuration value from the global configuration only
func (this *DatastoreConfigSnapshot) GetString_GlobalOnly(key string) (value string, err error) {
	if this.GlobalConfig == nil {
		return
	}

	// Otherwise, look up the key in the global configuration and return its value if found
	return this.GlobalConfig.GetString(key)
}

// Gets a boolean typed configuration value.
func (this *DatastoreConfigSnapshot) GetBool(key string) (value bool, err error) {
	// If a datastore-specific configuration is available
	if this.DedicatedConfig != nil {
		// Lookup the value for the given key
		value, err = this.DedicatedConfig.GetBool(key)

		// If the key was found
		if err == nil {
			// Return its value
			return
		}
	}

	if this.GlobalConfig == nil {
		return
	}

	// Otherwise, look up the key in the global configuration and return its value if found
	return this.GlobalConfig.GetBool(key)
}

// Gets a 64-bit integer typed configuration value.
func (this *DatastoreConfigSnapshot) GetInt64(key string) (value int64, err error) {
	// If a datastore-specific configuration is available
	if this.DedicatedConfig != nil {
		// Lookup the value for the given key
		value, err = this.DedicatedConfig.GetInt64(key)

		// If the key was found
		if err == nil {
			// Return its value
			return
		}
	}

	if this.GlobalConfig == nil {
		return
	}

	// Otherwise, look up the key in the global configuration and return its value if found
	return this.GlobalConfig.GetInt64(key)
}

// Gets a 64-bit float typed configuration value.
func (this *DatastoreConfigSnapshot) GetFloat64(key string) (value float64, err error) {
	// If a datastore-specific configuration is available
	if this.DedicatedConfig != nil {
		// Lookup the value for the given key
		value, err = this.DedicatedConfig.GetFloat64(key)

		// If the key was found
		if err == nil {
			// Return its value
			return
		}
	}

	if this.GlobalConfig == nil {
		return
	}

	// Otherwise, look up the key in the global configuration and return its value if found
	return this.GlobalConfig.GetFloat64(key)
}
