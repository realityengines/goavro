// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"bytes"
	"errors"
	"fmt"
)

// Union wraps a datum value in a map for encoding as a Union, as required by
// Union encoder.
//
// When providing a value for an Avro union, the encoder will accept `nil` for a
// `null` value. If the value is non-`nil`, it must be a
// `map[string]interface{}` with a single key-value pair, where the key is the
// Avro type name and the value is the datum's value. As a convenience, the
// `Union` function wraps any datum value in a map as specified above.
//
//     func ExampleUnion() {
//        codec, err := goavro.NewCodec(`["null","string","int"]`)
//        if err != nil {
//            fmt.Println(err)
//        }
//        buf, err := codec.TextualFromNative(nil, goavro.Union("string", "some string"))
//        if err != nil {
//            fmt.Println(err)
//        }
//        fmt.Println(string(buf))
//        // Output: {"string":"some string"}
//     }
func Union(name string, datum interface{}) interface{} {
	if datum == nil && name == "null" {
		return nil
	}
	return map[string]interface{}{name: datum}
}

func buildCodecForTypeDescribedBySlice(st map[string]*Codec, enclosingNamespace string, schemaArray []interface{}) (*Codec, error) {
	if len(schemaArray) == 0 {
		return nil, errors.New("Union ought to have one or more members")
	}

	allowedTypes := make([]string, len(schemaArray)) // used for error reporting when encoder receives invalid datum type
	codecFromIndex := make([]*Codec, len(schemaArray))
	codecFromName := make(map[string]*Codec, len(schemaArray))
	indexFromName := make(map[string]int, len(schemaArray))

	nullIndex := -1
	for i, unionMemberSchema := range schemaArray {
		unionMemberCodec, err := buildCodec(st, enclosingNamespace, unionMemberSchema)
		if err != nil {
			return nil, fmt.Errorf("Union item %d ought to be valid Avro type: %s", i+1, err)
		}
		fullName := unionMemberCodec.typeName.fullName
		if fullName == "null" {
			nullIndex = i
		}
		if _, ok := indexFromName[fullName]; ok {
			return nil, fmt.Errorf("Union item %d ought to be unique type: %s", i+1, unionMemberCodec.typeName)
		}
		allowedTypes[i] = fullName
		codecFromIndex[i] = unionMemberCodec
		codecFromName[fullName] = unionMemberCodec
		indexFromName[fullName] = i
	}

	isOptional := (nullIndex > -1) && (len(schemaArray) == 2)

	writeBinaryDatum := func(buf []byte, index int, datum interface{}) ([]byte, error) {
		c := codecFromIndex[index]
		buf, _ = longBinaryFromNative(buf, index)
		return c.binaryFromNative(buf, datum)
	}

	writeTextDatum := func(buf []byte, index int, key string, datum interface{}) ([]byte, error) {
		buf = append(buf, '{')
		var err error
		buf, err = stringTextualFromNative(buf, key)
		if err != nil {
			return nil, fmt.Errorf("cannot encode textual union: %s", err)
		}
		buf = append(buf, ':')
		c := codecFromIndex[index]
		buf, err = c.textualFromNative(buf, datum)
		if err != nil {
			return nil, fmt.Errorf("cannot encode textual union: %s", err)
		}
		return append(buf, '}'), nil
	}

	return &Codec{
		// NOTE: To support record field default values, union schema set to the
		// type name of first member
		// TODO: add/change to schemaCanonical below
		schemaOriginal: codecFromIndex[0].typeName.fullName,
		typeName:       &name{"union", nullNamespace},

		wrapDefault: func(defaultValue interface{}) (interface{}, error) {
			if isOptional {
				if (defaultValue == nil && nullIndex == 1) ||
					(defaultValue != nil && nullIndex == 0) {
					return nil, fmt.Errorf("default value ought to encode using first union type")
				}
				return defaultValue, nil
			}
			return Union(codecFromIndex[0].typeName.fullName, defaultValue), nil
		},

		nativeFromBinary: func(buf []byte) (interface{}, []byte, error) {
			var decoded interface{}
			var err error

			decoded, buf, err = longNativeFromBinary(buf)
			if err != nil {
				return nil, nil, err
			}
			index := decoded.(int64) // longDecoder always returns int64, so elide error checking
			if index < 0 || index >= int64(len(codecFromIndex)) {
				return nil, nil, fmt.Errorf("cannot decode binary union: index ought to be between 0 and %d; read index: %d", len(codecFromIndex)-1, index)
			}
			c := codecFromIndex[index]
			decoded, buf, err = c.nativeFromBinary(buf)
			if err != nil {
				return nil, nil, fmt.Errorf("cannot decode binary union item %d: %s", index+1, err)
			}
			if decoded == nil || isOptional {
				// do not wrap a nil value in a map
				return decoded, buf, nil
			}
			return Union(allowedTypes[index], decoded), buf, nil
		},
		binaryFromNative: func(buf []byte, datum interface{}) ([]byte, error) {
			if isOptional && datum != nil {
				return writeBinaryDatum(buf, 1-nullIndex, datum)
			}
			switch v := datum.(type) {
			case nil:
				if nullIndex == -1 {
					return nil, fmt.Errorf("cannot encode binary union: no member schema types support datum: allowed types: %v; received: %T", allowedTypes, datum)
				}
				return longBinaryFromNative(buf, nullIndex)
			case map[string]interface{}:
				if len(v) != 1 {
					return nil, fmt.Errorf("cannot encode binary union: non-nil Union values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; received: %T", allowedTypes, datum)
				}
				// will execute exactly once
				for key, value := range v {
					index, ok := indexFromName[key]
					if !ok {
						return nil, fmt.Errorf("cannot encode binary union: no member schema types support datum: allowed types: %v; received: %T", allowedTypes, datum)
					}
					return writeBinaryDatum(buf, index, value)
				}
			}
			return nil, fmt.Errorf("cannot encode binary union: non-nil Union values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; received: %T", allowedTypes, datum)
		},
		nativeFromTextual: func(buf []byte) (interface{}, []byte, error) {
			if len(buf) >= 4 && bytes.Equal(buf[:4], []byte("null")) {
				if _, ok := indexFromName["null"]; ok {
					return nil, buf[4:], nil
				}
			}

			var datum interface{}
			var err error
			datum, buf, err = genericMapTextDecoder(buf, nil, codecFromName)
			if isOptional && datum != nil {
				index := 1 - nullIndex
				if wrapper, ok := datum.(map[string]interface{}); !ok || len(wrapper) != 1 {
					err = fmt.Errorf("expected a map with exactly one element")
				} else if datum, ok = wrapper[allowedTypes[index]]; !ok {
					err = fmt.Errorf("expected %s in union map", allowedTypes[index])
				}
			}
			if err != nil {
				return nil, nil, fmt.Errorf("cannot decode textual union: %s", err)
			}

			return datum, buf, nil
		},
		textualFromNative: func(buf []byte, datum interface{}) ([]byte, error) {
			if isOptional && datum != nil {
				index := 1 - nullIndex
				return writeTextDatum(buf, index, allowedTypes[index], datum)
			}
			switch v := datum.(type) {
			case nil:
				_, ok := indexFromName["null"]
				if !ok {
					return nil, fmt.Errorf("cannot encode textual union: no member schema types support datum: allowed types: %v; received: %T", allowedTypes, datum)
				}
				return append(buf, "null"...), nil
			case map[string]interface{}:
				if len(v) != 1 {
					return nil, fmt.Errorf("cannot encode textual union: non-nil Union values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; received: %T", allowedTypes, datum)
				}
				// will execute exactly once
				for key, value := range v {
					index, ok := indexFromName[key]
					if !ok {
						return nil, fmt.Errorf("cannot encode textual union: no member schema types support datum: allowed types: %v; received: %T", allowedTypes, datum)
					}
					return writeTextDatum(buf, index, key, value)
				}
			}
			return nil, fmt.Errorf("cannot encode textual union: non-nil values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; received: %T", allowedTypes, datum)
		},
	}, nil
}
