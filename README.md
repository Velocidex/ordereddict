# ordereddict
A simple Ordered Dict implementation.

## YAML

YAML serialization uses `go.yaml.in/yaml/v4` and requires Go 1.18 or newer.
Mappings are emitted in dictionary order, including dictionaries nested in
lists, maps and structs. Decoding a mapping into a `Dict` preserves nested
mapping order using `*Dict` values, including mappings inside sequences,
aliases, and merge keys (`<<`).

This changes the YAML hooks from the previous v2 implementation:
`MarshalYAML` returns a v4 `*yaml.Node` instead of a v2 `MapSlice`, and
`UnmarshalYAML` accepts a v4 `*yaml.Node`. Consumers must use v4 when
serializing dictionaries as YAML. JSON serialization is unchanged.
