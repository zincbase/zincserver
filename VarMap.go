package main

import (
	"encoding/json"
	"errors"
)

type VarMap struct {
	entries map[string]interface{}
}

func NewVarMap(entries map[string]interface{}) *VarMap {
	return &VarMap{entries: entries}
}

func NewEmptyVarMap() *VarMap {
	return &VarMap{entries: map[string]interface{}{}}
}

var ErrNotFound = errors.New("Key not found")
var ErrUnexpectedType = errors.New("Unexpected type")

// Read only operations
func (this *VarMap) GetString(key string) (string, error) {
	val, exists := this.entries[key]
	if !exists {
		return "", ErrNotFound
	}

	result, ok := val.(string)
	if ok {
		return result, nil
	} else {
		return "", ErrUnexpectedType
	}
}

func (this *VarMap) GetBool(key string) (bool, error) {
	val, exists := this.entries[key]
	if !exists {
		return false, ErrNotFound
	}

	result, ok := val.(bool)
	if ok {
		return result, nil
	} else {
		return false, ErrUnexpectedType
	}
}

func (this *VarMap) GetInt64(key string) (int64, error) {
	val, err := this.GetFloat64(key)
	if err != nil {
		return 0, err
	}

	return int64(val), nil
}

func (this *VarMap) GetFloat64(key string) (float64, error) {
	val, exists := this.entries[key]
	if !exists {
		return 0, ErrNotFound
	}

	result, ok := val.(float64)
	if ok {
		return result, nil
	} else {
		return 0, ErrUnexpectedType
	}
}

// Operations with fallback keys
func (this *VarMap) GetFirstMatchString(keys ...string) (string, error) {
	for i := range keys {
		val, err := this.GetString(keys[i])

		if err == nil {
			return val, nil
		}
	}

	return "", ErrNotFound
}

func (this *VarMap) GetFirstMatchBool(keys ...string) (bool, error) {
	for i := range keys {
		val, err := this.GetBool(keys[i])

		if err == nil {
			return val, nil
		}
	}

	return false, ErrNotFound
}

func (this *VarMap) GetFirstMatchInt64(keys ...string) (int64, error) {
	for i := range keys {
		val, err := this.GetInt64(keys[i])

		if err == nil {
			return val, nil
		}
	}

	return 0, ErrNotFound
}

func (this *VarMap) GetFirstMatchFloat64(keys ...string) (float64, error) {
	for i := range keys {
		val, err := this.GetFloat64(keys[i])

		if err == nil {
			return val, nil
		}
	}

	return 0, ErrNotFound
}

func (this *VarMap) GetAny(key string) (interface{}, error) {
	val, exists := this.entries[key]
	if !exists {
		return false, ErrNotFound
	}

	return val, nil
}

/*
func (this *VarMap) GetMap(key string) (map[string]interface{}, error) {
	val, exists := this.entries[key]
	if !exists {
		return nil, ErrNotFound
	}

	result, ok := val.(map[string]interface{})
	if ok {
		return result, nil
	} else {
		return nil, ErrUnexpectedType
	}
}

func (this *VarMap) GetVariantMap(key string) (*VarMap, error) {
	val, err := this.GetMap(key)
	if err != nil {
		return nil, err
	}

	return NewVarMap(val), nil
}

func (this *VarMap) GetArray(key string) ([]interface{}, error) {
	val, exists := this.entries[key]
	if !exists {
		return nil, ErrNotFound
	}

	result, ok := val.([]interface{})
	if ok {
		return result, nil
	} else {
		return nil, ErrUnexpectedType
	}
}

func (this *VarMap) FindKeysStartingWith(prefix string) (results []string) {
	for key, _ := range this.entries {
		if strings.HasPrefix(key, prefix) {
			results = append(results, key)
		}
	}

	return
}
*/

func (this *VarMap) Has(key string) bool {
	_, ok := this.entries[key]
	return ok
}

func (this *VarMap) Keys() (results []string) {
	for key, _ := range this.entries {
		results = append(results, key)
	}

	return
}

func (this *VarMap) Count() int {
	return len(this.entries)
}

func (this *VarMap) Clone() (clone *VarMap) {
	clone = NewEmptyVarMap()

	for key, value := range this.entries {
		clone.entries[key] = value
	}

	return
}

// Destructive operations
func (this *VarMap) Set(key string, value interface{}) {
	this.entries[key] = value
}

func (this *VarMap) SetFromJsonKeyAndValue(jsonKeyBytes []byte, jsonValueBytes []byte) (err error) {
	var key string
	err = json.Unmarshal(jsonKeyBytes, &key)
	if err != nil {
		return
	}

	if len(jsonValueBytes) == 0 {
		this.Delete(key)
		return
	}

	var value interface{}
	err = json.Unmarshal(jsonValueBytes, &value)
	if err != nil {
		return
	}

	this.Set(key, value)
	return
}

func (this *VarMap) AppendJsonEntries(jsonEntries []Entry) (err error) {
	for _, entry := range jsonEntries {
		if entry.Header.KeyFormat != DataFormat_JSON || entry.Header.ValueFormat != DataFormat_JSON {
			continue
		}

		err = this.SetFromJsonKeyAndValue(entry.Key, entry.Value)

		if err != nil {
			return
		}
	}

	return
}

func (this *VarMap) Delete(key string) {
	delete(this.entries, key)
}

func (this *VarMap) Clear() {
	for key, _ := range this.entries {
		delete(this.entries, key)
	}
}

func (this *VarMap) Append(newEntriesMap *VarMap) {
	for key, value := range newEntriesMap.entries {
		this.entries[key] = value
	}
}

func (this *VarMap) Rewrite(newEntries *VarMap) {
	this.entries = newEntries.entries
}
