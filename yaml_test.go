package ordereddict

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
)

func TestYAMLRoundTrip(t *testing.T) {
	input := NewDict().Set("z", "Y").Set("rows", []interface{}{
		NewDict().Set("z", "first").Set("a", "second"),
	}).Set("nested", NewDict().Set("b", 2).Set("a", 1)).
		Set("Y", "legacy_key").Set("123", "numeric_key")

	data, err := yaml.Dump(input, yaml.WithV2Defaults(), yaml.WithCompactSeqIndent())
	require.NoError(t, err)
	require.Equal(t, "z: \"Y\"\nrows:\n- z: first\n  a: second\nnested:\n  b: 2\n  a: 1\n\"Y\": legacy_key\n\"123\": numeric_key\n", string(data))

	output := NewDict()
	require.NoError(t, yaml.Unmarshal(data, output))
	require.Equal(t, input.String(), output.String())

	again, err := yaml.Dump(output, yaml.WithV2Defaults(), yaml.WithCompactSeqIndent())
	require.NoError(t, err)
	require.Equal(t, string(data), string(again))
}

func TestYAMLZeroValueAndNilDict(t *testing.T) {
	// Zero-value Dict and nil *Dict field inside a struct allocated by go-yaml via reflect.New.
	var zero Dict
	require.NoError(t, yaml.Unmarshal([]byte("b: 2\na: 1\n"), &zero))
	require.Equal(t, []string{"b", "a"}, zero.Keys())

	type wrapper struct {
		Meta  *Dict `json:"meta"`
		Empty *Dict `json:"empty"`
	}
	var w wrapper
	require.NoError(t, yaml.Load([]byte("meta:\n  z: 1\n  a: 2\nempty: null\n"), &w, yaml.WithV2Defaults()))
	require.NotNil(t, w.Meta)
	require.Equal(t, []string{"z", "a"}, w.Meta.Keys())
	require.Nil(t, w.Empty)

	// Typed nil *Dict both directly and nested inside a Dict.
	var nilDict *Dict
	rawNil, err := nilDict.MarshalYAML()
	require.NoError(t, err)
	require.Nil(t, rawNil)

	withNil := NewDict().Set("sub", (*Dict)(nil)).Set("ok", 1)
	out, err := yaml.Marshal(withNil)
	require.NoError(t, err)
	reloaded := NewDict()
	require.NoError(t, yaml.Unmarshal(out, reloaded))
	require.Equal(t, `{"sub":null,"ok":1}`, reloaded.String())
}

func TestYAMLAliasesAndMergeKeys(t *testing.T) {
	src := `
base: &base
  z: 1
  a: 2
extra: &extra
  a: 99
  m: 3
alias_copy: *base
merged:
  b: before
  <<: [*base, *extra]
  z: 10
`
	got := NewDict()
	require.NoError(t, yaml.Load([]byte(src), got, yaml.WithV2Defaults()))

	aliasCopy, ok := got.Get("alias_copy")
	require.True(t, ok)
	aliasDict, ok := aliasCopy.(*Dict)
	require.True(t, ok, "alias target should decode as *Dict, got %T", aliasCopy)
	require.Equal(t, []string{"z", "a"}, aliasDict.Keys())

	mergedVal, ok := got.Get("merged")
	require.True(t, ok)
	mergedDict, ok := mergedVal.(*Dict)
	require.True(t, ok)
	// Explicit keys ("b", "z") win over merged keys; earlier merge source (*base) wins for "a".
	require.Equal(t, `{"b":"before","a":2,"m":3,"z":10}`, mergedDict.String())
}

func TestYAMLInvalidAndCyclicValues(t *testing.T) {
	require.Error(t, yaml.Unmarshal([]byte("rows: [!!int invalid]"), NewDict()))
	require.Error(t, yaml.Unmarshal([]byte("[one, two]"), NewDict()))
	require.Error(t, yaml.Unmarshal([]byte("value: &self [*self]"), NewDict()))
	require.Error(t, yaml.Unmarshal([]byte("dup: 1\ndup: 2\n"), NewDict()))
	require.Error(t, yaml.Unmarshal([]byte("<<: not_a_map\na: 1\n"), NewDict()))

	// Direct self-cycle on Marshal must return an error instead of stack-overflowing.
	direct := NewDict()
	direct.Set("self", direct)
	_, err := yaml.Marshal(direct)
	require.Error(t, err)
}

func TestYAMLEmptyAndScalarValues(t *testing.T) {
	input := NewDict().Set("empty", NewDict()).Set("nil", nil).
		Set("boolean", true).Set("number", 42)
	data, err := yaml.Marshal(input)
	require.NoError(t, err)
	output := NewDict()
	require.NoError(t, yaml.Unmarshal(data, output))
	require.Equal(t, input.String(), output.String())
}

func TestYAMLNestedInStructAndMap(t *testing.T) {
	input := struct {
		Data map[string]interface{} `yaml:"data"`
	}{Data: map[string]interface{}{"item": NewDict().Set("z", 1).Set("a", 2)}}
	data, err := yaml.Dump(input, yaml.WithV2Defaults())
	require.NoError(t, err)
	require.Contains(t, string(data), "z: 1\n")
	require.NotContains(t, string(data), "key:")
}
