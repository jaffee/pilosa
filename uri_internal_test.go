// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import "testing"

func TestDefaultURI(t *testing.T) {
	uri := DefaultURI()
	compare(t, uri, "http", "localhost", 10101)
}

func TestURIWithHostPort(t *testing.T) {
	uri, err := NewURIFromHostPort("index1.pilosa.com", 3333)
	if err != nil {
		t.Fatal(err)
	}
	compare(t, uri, "http", "index1.pilosa.com", 3333)
}

func TestURIWithInvalidHostPort(t *testing.T) {
	_, err := NewURIFromHostPort("index?.pilosa.com", 3333)
	if err == nil {
		t.Fatalf("should have failed")
	}
}

func TestNewURIFromAddress(t *testing.T) {
	for _, item := range validFixture() {
		uri, err := NewURIFromAddress(item.address)
		if err != nil {
			t.Fatalf("Can't parse address: %s, %s", item.address, err)
		}
		compare(t, uri, item.scheme, item.host, item.port)
	}
}

func TestNewURIFromAddressInvalidAddress(t *testing.T) {
	for _, addr := range invalidFixture() {
		_, err := NewURIFromAddress(addr)
		if err == nil {
			t.Fatalf("Invalid address should return an error: %s", addr)
		}
	}
}

func TestNormalizedAddress(t *testing.T) {
	uri, err := NewURIFromAddress("http+protobuf://big-data.pilosa.com:6888")
	if err != nil {
		t.Fatalf("Can't parse address")
	}
	if uri.Normalize() != "http://big-data.pilosa.com:6888" {
		t.Fatalf("Normalized address is not normal")
	}
}

func TestURIPath(t *testing.T) {
	uri, err := NewURIFromAddress("http+protobuf://big-data.pilosa.com:6888")
	if err != nil {
		t.Fatal(err)
	}
	target := "http://big-data.pilosa.com:6888/index/foo"
	if uri.Path("/index/foo") != target {
		t.Fatalf("%s != %s", uri.Path("/index/foo"), target)
	}
}

func TestEquals(t *testing.T) {
	uri1 := DefaultURI()
	if uri1.Equals(nil) {
		t.Fatalf("URI should not be equal to nil")
	}
	if !uri1.Equals(DefaultURI()) {
		t.Fatalf("URI should be equal to another URI with the same scheme, host and port")
	}
}

func TestSetScheme(t *testing.T) {
	uri := DefaultURI()
	target := "fun"
	err := uri.SetScheme(target)
	if err != nil {
		t.Fatal(err)
	}
	if uri.GetScheme() != target {
		t.Fatalf("%s != %s", uri.GetScheme(), target)
	}
}

func TestSetHost(t *testing.T) {
	uri := DefaultURI()
	target := "10.20.30.40"
	err := uri.SetHost(target)
	if err != nil {
		t.Fatal(err)
	}
	if uri.GetHost() != target {
		t.Fatalf("%s != %s", uri.Host, target)
	}
}

func TestSetPort(t *testing.T) {
	uri := DefaultURI()
	target := uint16(9999)
	uri.SetPort(target)
	if uri.GetPort() != target {
		t.Fatalf("%d != %d", uri.Port, target)
	}
}

func TestSetInvalidScheme(t *testing.T) {
	uri := DefaultURI()
	err := uri.SetScheme("?invalid")
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestSetInvalidHost(t *testing.T) {
	uri := DefaultURI()
	err := uri.SetHost("index?.pilosa.com")
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestHostPort(t *testing.T) {
	uri, err := NewURIFromHostPort("i.pilosa.com", 15001)
	if err != nil {
		t.Fatal(err)
	}
	target := "i.pilosa.com:15001"
	if uri.HostPort() != target {
		t.Fatalf("%s != %s", uri.HostPort(), target)
	}
}

func compare(t *testing.T, uri *URI, scheme string, host string, port uint16) {
	if uri.GetScheme() != scheme {
		t.Fatalf("Scheme does not match: %s != %s", uri.Scheme, scheme)
	}
	if uri.GetHost() != host {
		t.Fatalf("Host does not match: %s != %s", uri.Host, host)
	}
	if uri.GetPort() != port {
		t.Fatalf("Port does not match: %d != %d", uri.Port, port)
	}
}

type uriItem struct {
	address string
	scheme  string
	host    string
	port    uint16
}

func validFixture() []uriItem {
	var test = []uriItem{
		{"http+protobuf://index1.pilosa.com:3333", "http+protobuf", "index1.pilosa.com", 3333},
		{"index1.pilosa.com:3333", "http", "index1.pilosa.com", 3333},
		{"https://index1.pilosa.com", "https", "index1.pilosa.com", 10101},
		{"index1.pilosa.com", "http", "index1.pilosa.com", 10101},
		{"https://:3333", "https", "localhost", 3333},
		{":3333", "http", "localhost", 3333},
		{"[::1]", "http", "[::1]", 10101},
		{"[::1]:3333", "http", "[::1]", 3333},
		{"[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]:3333", "http", "[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]", 3333},
		{"https://[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]:3333", "https", "[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]", 3333},
	}
	return test
}

func invalidFixture() []string {
	return []string{"foo:bar", "http://foo:", "foo:", ":bar", "http://pilosa.com:129999999999999999999999993", "fd42:4201:f86b:7e09:216:3eff:fefa:ed80"}
}