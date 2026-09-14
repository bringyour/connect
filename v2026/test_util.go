// test_util.go — shared test assertions so the modules that depend on connect
// (server, sdk, proxy) do not each pull in a third-party assert library. Like
// server/test_util.go this imports testing from a non-test file, and it lives
// in package connect (not a subpackage) so connect's own tests can use it
// without a package importing its own child (see CODESTYLE package layering).
//
// AssertEqual/AssertNotEqual replace go-playground/assert/v2's Equal/NotEqual
// one-for-one: they FailNow on mismatch (abort like require, not accumulate)
// and use the same pointer- and typed-nil-aware equality, so the existing call
// sites behave identically. The equality logic is adapted from
// go-playground/assert/v2 (MIT license, Copyright (c) 2015 Dean Karn).
package connect

import (
	"fmt"
	"path"
	"reflect"
	"runtime"
	"testing"
)

// AssertEqual fails the test (FailNow) when v1 is not equal to v2, reporting the
// caller's file:line. A pointer is compared by its pointee (one level), and a
// typed nil is treated as nil, before falling back to reflect.DeepEqual.
func AssertEqual(t testing.TB, v1 any, v2 any) {
	if !assertIsEqual(v1, v2) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("%s:%d %v does not equal %v\n", path.Base(file), line, v1, v2)
		t.FailNow()
	}
}

// AssertNotEqual fails the test (FailNow) when v1 IS equal to v2. See AssertEqual
// for the equality semantics.
func AssertNotEqual(t testing.TB, v1 any, v2 any) {
	if assertIsEqual(v1, v2) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("%s:%d %v should not be equal %v\n", path.Base(file), line, v1, v2)
		t.FailNow()
	}
}

// assertIsEqual reports whether v1 equals v2, dereferencing one pointer level and
// treating a typed-nil chan/func/interface/map/ptr/slice as nil, then comparing
// with reflect.DeepEqual. Ported verbatim (behavior-preserving) from
// go-playground/assert/v2 so the thousands of migrated call sites are unchanged.
func assertIsEqual(val1 any, val2 any) bool {
	v1 := reflect.ValueOf(val1)
	v2 := reflect.ValueOf(val2)
	v1Nil := !v1.IsValid() || reflectValueIsNil(v1)
	v2Nil := !v2.IsValid() || reflectValueIsNil(v2)
	if v1Nil || v2Nil {
		return v1Nil && v2Nil
	}

	if v1.Kind() == reflect.Ptr {
		v1 = v1.Elem()
	}
	if v2.Kind() == reflect.Ptr {
		v2 = v2.Elem()
	}

	if !v1.IsValid() && !v2.IsValid() {
		return true
	}

	switch v1.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		if v1.IsNil() {
			v1 = reflect.ValueOf(nil)
		}
	}
	switch v2.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		if v2.IsNil() {
			v2 = reflect.ValueOf(nil)
		}
	}

	v1Underlying := reflect.Zero(reflect.TypeOf(v1)).Interface()
	v2Underlying := reflect.Zero(reflect.TypeOf(v2)).Interface()

	if v1 == v1Underlying {
		if v2 == v2Underlying {
			goto CASE4
		} else {
			goto CASE3
		}
	} else {
		if v2 == v2Underlying {
			goto CASE2
		} else {
			goto CASE1
		}
	}

CASE1:
	return reflect.DeepEqual(v1.Interface(), v2.Interface())
CASE2:
	return reflect.DeepEqual(v1.Interface(), v2)
CASE3:
	return reflect.DeepEqual(v1, v2.Interface())
CASE4:
	return reflect.DeepEqual(v1, v2)
}

// reflectValueIsNil reports nil only for reflection kinds that permit IsNil.
func reflectValueIsNil(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
