package ordereddict

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Velocidex/json"
	"github.com/Velocidex/yaml/v2"
)

var (
	// Mark the item as deleted
	Deleted = 1

	CompactionThreashold = 10

	// Keep stats about compactions - mostly used for tests
	Compactions uint64
)

type Item struct {
	Key   string
	Value interface{}
}

func (self Item) isDeleted() bool {
	return self.Value == &Deleted
}

// A concerete implementation of a row - similar to Python's
// OrderedDict.  Main difference is that delete is implemented by
// recording a deletion - this means it is slightly less memory
// efficient to delete as we expect deletes to be relatively rare.
type Dict struct {
	sync.Mutex

	items []Item

	// Map key -> item index
	store map[string]int

	case_insensitive bool

	// Used in Get() when we fail to match
	default_value interface{}

	// If set, this allows a field to be aliased:
	// 1. The alias does not show in regular iterations Items() or Keys()
	// 2. The Get() method will retried the aliased field when the alias is set.
	aliases map[string]string
}

func NewDict() *Dict {
	return &Dict{
		store: make(map[string]int),
	}
}

// Clear the dict
func (self *Dict) Clear() {
	self.Lock()
	defer self.Unlock()

	self.items = nil
	self.store = make(map[string]int)
}

func (self *Dict) AliasField(src, dest string) *Dict {
	self.Lock()
	defer self.Unlock()

	if self.aliases == nil {
		self.aliases = make(map[string]string)
	}

	self.aliases[self.getKey(src)] = self.getKey(dest)

	return self
}

func (self *Dict) DebugString() string {
	return fmt.Sprintf("Keys %v, len(store) %v, case_insensitive %v default_value %v\n",
		self.Keys(), self.Len(), self.IsCaseInsensitive(), self.GetDefault())
}

func (self *Dict) IsCaseInsensitive() bool {
	self.Lock()
	defer self.Unlock()

	return self.case_insensitive
}

func (self *Dict) getKey(key string) string {
	if self.case_insensitive {
		return strings.ToLower(key)
	}
	return key
}

func (self *Dict) MergeFrom(other *Dict) {
	for _, item := range other.Items() {
		self.Set(item.Key, item.Value)
	}
	self.default_value = other.default_value
	self.case_insensitive = other.case_insensitive
}

func (self *Dict) SetDefault(value interface{}) *Dict {
	self.Lock()
	defer self.Unlock()

	self.default_value = value
	return self
}

func (self *Dict) GetDefault() interface{} {
	self.Lock()
	defer self.Unlock()

	return self.default_value
}

func (self *Dict) SetCaseInsensitive() *Dict {
	res := &Dict{
		case_insensitive: true,
		store:            make(map[string]int),
		default_value:    self.default_value,
	}

	for _, item := range self.Items() {
		if item.isDeleted() {
			continue
		}
		res.Set(item.Key, item.Value)
	}

	return res
}

func (self *Dict) Copy() *Dict {
	res := NewDict()
	res.items = append([]Item{}, self.items...)
	for k, v := range self.store {
		res.store[k] = v
	}
	res.case_insensitive = self.case_insensitive
	res.default_value = self.default_value
	return res
}

// Mark the item as deleted - we dont expect this to be too often.
func (self *Dict) Delete(key string) {
	self.Lock()
	defer self.Unlock()

	idx, pres := self.store[self.getKey(key)]
	if pres {
		delete(self.store, key)

		// Mark the item as deleted
		self.items[idx].Value = &Deleted
		self.maybeCompact()
	}
}

// Like Set() but does not effect the order. This also does not
// trigger any compactions and is more efficient then Set().
func (self *Dict) Update(key string, value interface{}) *Dict {
	self.Lock()
	defer self.Unlock()

	norm_key := self.getKey(key)
	idx, pres := self.store[norm_key]
	if pres {
		self.items[idx].Value = value

	} else {
		self.store[norm_key] = len(self.items)
		self.items = append(self.items, Item{
			Key:   key,
			Value: value,
		})
	}

	return self
}

// Set inserts the value into the dict.  This function also reorders
// the dict to make the new key the latest. If the key is already
// present, this is equivalent to a Delete() on the old key and an
// Update().  If the caller calls Set repeatadly on the same key this
// can result in a lot of compactions. In this case consider using
// Update() instead.
func (self *Dict) Set(key string, value interface{}) *Dict {
	self.Lock()
	defer self.Unlock()

	return self.set(key, value)
}

// Set always updates key order to the end.
func (self *Dict) set(key string, value interface{}) *Dict {
	norm_key := self.getKey(key)

	// If the item already exists, remove it then insert at the end.
	idx, pres := self.store[norm_key]
	if pres {
		self.items[idx].Value = &Deleted
	}

	self.store[norm_key] = len(self.items)
	self.items = append(self.items, Item{
		Key:   key,
		Value: value,
	})
	self.maybeCompact()

	return self
}

func (self *Dict) maybeCompact() {
	// Allow the items array to have up to 10 deletions before
	// compaction.
	if len(self.items)-len(self.store) < CompactionThreashold {
		return
	}

	new_store := make(map[string]int)
	new_items := []Item{}
	for _, item := range self.items {
		if item.isDeleted() {
			continue
		}

		norm_key := self.getKey(item.Key)

		new_store[norm_key] = len(new_items)
		new_items = append(new_items, Item{
			Key:   item.Key,
			Value: item.Value,
		})
	}

	self.store = new_store
	self.items = new_items
	atomic.AddUint64(&Compactions, 1)
}

func (self *Dict) Len() int {
	self.Lock()
	defer self.Unlock()

	return len(self.store)
}

func (self *Dict) Get(key string) (interface{}, bool) {
	self.Lock()
	defer self.Unlock()

	store_key := self.getKey(key)
	idx, pres := self.store[store_key]
	if !pres {
		// Is there an alias for this field?
		if self.aliases != nil {
			dest, pres := self.aliases[store_key]
			if pres {
				idx, pres := self.store[dest]
				if pres {
					return self.items[idx].Value, true
				}
			}
		}

		if self.default_value != nil {
			return self.default_value, false
		}
		return nil, false
	}

	return self.items[idx].Value, true
}

func (self *Dict) GetString(key string) (string, bool) {
	v, pres := self.Get(key)
	if pres {
		v_str, ok := to_string(v)
		if ok {
			return v_str, true
		}
	}
	return "", false
}

func (self *Dict) GetBool(key string) (bool, bool) {
	v, pres := self.Get(key)
	if pres {
		v_bool, ok := v.(bool)
		if ok {
			return v_bool, true
		}
	}
	return false, false
}

func to_string(x interface{}) (string, bool) {
	switch t := x.(type) {
	case string:
		return t, true
	case *string:
		return *t, true
	case []byte:
		return string(t), true
	default:
		return "", false
	}
}

func (self *Dict) GetStrings(key string) ([]string, bool) {
	v, pres := self.Get(key)
	if pres && v != nil {
		slice := reflect.ValueOf(v)
		if slice.Type().Kind() == reflect.Slice {
			result := []string{}
			for i := 0; i < slice.Len(); i++ {
				value := slice.Index(i).Interface()
				item, ok := to_string(value)
				if ok {
					result = append(result, item)
				}
			}
			return result, true
		}
	}
	return nil, false
}

func (self *Dict) GetInt64(key string) (int64, bool) {
	value, pres := self.Get(key)
	if pres {
		switch t := value.(type) {
		case int:
			return int64(t), true
		case int8:
			return int64(t), true
		case int16:
			return int64(t), true
		case int32:
			return int64(t), true
		case int64:
			return int64(t), true
		case uint8:
			return int64(t), true
		case uint16:
			return int64(t), true
		case uint32:
			return int64(t), true
		case uint64:
			return int64(t), true
		case float32:
			return int64(t), true
		case float64:
			return int64(t), true
		}
	}
	return 0, false
}

func (self *Dict) Keys() (res []string) {
	self.Lock()
	defer self.Unlock()

	res = make([]string, 0, len(self.items))
	for _, i := range self.items {
		if i.isDeleted() {
			continue
		}

		res = append(res, i.Key)
	}

	return res
}

func (self *Dict) Items() []Item {
	self.Lock()
	defer self.Unlock()

	res := make([]Item, 0, len(self.items))
	for _, i := range self.items {
		if i.isDeleted() {
			continue
		}

		res = append(res, i)
	}

	return res
}

func (self *Dict) Values() []interface{} {
	self.Lock()
	defer self.Unlock()

	res := make([]interface{}, 0, len(self.items))
	for _, i := range self.items {
		if i.isDeleted() {
			continue
		}

		res = append(res, i.Value)
	}

	return res
}

// Convert to Golang native map type (unordered)
func (self *Dict) ToMap() map[string]interface{} {
	result := make(map[string]interface{})

	for _, item := range self.Items() {
		result[item.Key] = item.Value
	}

	return result
}

// Printing the dict will always result in a valid JSON document.
func (self *Dict) String() string {
	serialized, err := self.MarshalJSON()
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}
	return string(serialized)
}

func (self *Dict) GoString() string {
	return self.String()
}

func (self *Dict) UnmarshalYAML(unmarshal func(interface{}) error) error {
	m := yaml.MapSlice{}
	err := unmarshal(&m)
	if err != nil {
		return err
	}

	for _, item := range m {
		key, ok := item.Key.(string)
		if ok {
			self.Set(key, item.Value)
		}
	}
	return nil
}

// this implements type json.Unmarshaler interface, so can be called
// in json.Unmarshal(data, om). We preserve key order when
// unmarshaling from JSON.
func (self *Dict) UnmarshalJSON(data []byte) error {
	self.Lock()
	defer self.Unlock()

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	// must open with a delim token '{'
	t, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok || delim != '{' {
		return fmt.Errorf("expect JSON object open with '{'")
	}

	err = self.parseobject(dec)
	if err != nil {
		return err
	}

	t, err = dec.Token()
	if err != io.EOF {
		return fmt.Errorf("expect end of JSON object but got more token: %T: %v or err: %v", t, t, err)
	}

	return nil
}

func (self *Dict) parseobject(dec *json.Decoder) (err error) {
	var t json.Token
	for dec.More() {
		t, err = dec.Token()
		if err != nil {
			return err
		}

		key, ok := t.(string)
		if !ok {
			return fmt.Errorf("expecting JSON key should be always a string: %T: %v", t, t)
		}

		t, err = dec.Token()
		if err == io.EOF {
			break

		} else if err != nil {
			return err
		}

		var value interface{}
		value, err = handledelim(t, dec)
		if err != nil {
			return err
		}
		if self.store == nil {
			self.store = make(map[string]int)
		}
		self.set(key, value)
	}

	t, err = dec.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok || delim != '}' {
		return fmt.Errorf("expect JSON object close with '}'")
	}

	return nil
}

func parsearray(dec *json.Decoder) (arr []interface{}, err error) {
	var t json.Token
	arr = make([]interface{}, 0)
	for dec.More() {
		t, err = dec.Token()
		if err != nil {
			return
		}

		var value interface{}
		value, err = handledelim(t, dec)
		if err != nil {
			return
		}
		arr = append(arr, value)
	}
	t, err = dec.Token()
	if err != nil {
		return
	}
	delim, ok := t.(json.Delim)

	if !ok || delim != ']' {
		err = fmt.Errorf("expect JSON array close with ']'")
		return
	}

	return
}

func handledelim(token json.Token, dec *json.Decoder) (res interface{}, err error) {
	switch t := token.(type) {
	case json.Delim:
		switch t {
		case '{':
			dict2 := NewDict()
			err = dict2.parseobject(dec)
			if err != nil {
				return
			}
			return dict2, nil
		case '[':
			var value []interface{}
			value, err = parsearray(dec)
			if err != nil {
				return
			}
			return value, nil
		default:
			return nil, fmt.Errorf("Unexpected delimiter: %q", t)
		}

	case string:
		// does it look like a timestamp in RFC3339
		if len(t) >= 20 && t[10] == 'T' {
			// Attempt to convert it from timestamp.
			parsed, err := time.Parse(time.RFC3339, t)
			if err == nil {
				return parsed, nil
			}
		}

		return t, nil

	case json.Number:
		value_str := t.String()

		// Try to parse as Uint
		value_uint, err := strconv.ParseUint(value_str, 10, 64)
		if err == nil {
			return value_uint, nil
		}

		value_int, err := strconv.ParseInt(value_str, 10, 64)
		if err == nil {
			return value_int, nil
		}

		// Failing this, try a float
		float, err := strconv.ParseFloat(value_str, 64)
		if err == nil {
			return float, nil
		}

		return nil, fmt.Errorf("Unexpected token: %v", token)
	}
	return token, nil
}

// Preserve key order when marshalling to JSON.
func (self *Dict) MarshalJSON() ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.Write([]byte("{"))
	for _, item := range self.Items() {

		// add key
		kEscaped, err := json.Marshal(item.Key)
		if err != nil {
			continue
		}

		// Check for back references and skip them - this is not perfect.
		subdict, ok := item.Value.(*Dict)
		if ok && subdict == self {
			continue
		}

		buf.Write(kEscaped)
		buf.Write([]byte(":"))

		vBytes, err := json.Marshal(item.Value)
		if err == nil {
			buf.Write(vBytes)
			buf.Write([]byte(","))
		} else {
			buf.Write([]byte("null,"))
		}
	}
	if len(self.store) > 0 {
		buf.Truncate(buf.Len() - 1)
	}
	buf.Write([]byte("}"))
	return buf.Bytes(), nil
}

func (self *Dict) MarshalYAML() (interface{}, error) {
	result := yaml.MapSlice{}
	for _, item := range self.Items() {
		result = append(result, yaml.MapItem{
			Key:   item.Key,
			Value: item.Value,
		})
	}

	return result, nil
}
