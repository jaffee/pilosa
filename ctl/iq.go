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

package ctl

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
	"github.com/pkg/errors"
)

// IQCommand represents a command for performing internal queries against pilosa
// for debugging.
type IQCommand struct {
	Host  string
	Index string
	Query string

	*pilosa.CmdIO
}

// NewIQCommand returns a new instance of IQCommand.
func NewIQCommand(stdin io.Reader, stdout, stderr io.Writer) *IQCommand {
	return &IQCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run runs the specified query and prints the results.
func (cmd *IQCommand) Run(ctx context.Context) error {
	r, err := http.Get("http://" + cmd.Host + "/status")
	if err != nil {
		return errors.Wrap(err, "getting status")
	}
	defer r.Body.Close()
	bod, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return errors.Wrap(err, "reading whole body")
	}
	dec := json.NewDecoder(bytes.NewReader(bod))
	type Status struct {
		Status *internal.ClusterStatus `json:"status"`
	}
	status := &Status{}
	err = dec.Decode(status)
	if err != nil {
		return errors.Wrap(err, "decoding status")
	}
	var nodeStatus *internal.NodeStatus
	for _, nodeStatus = range status.Status.Nodes {
		if nodeStatus.Host == cmd.Host {
			break
		}
	}
	if nodeStatus == nil {
		return errors.Errorf("Couldn't find host %v in \n%#v", cmd.Host, status.Status.Nodes)
	}
	var index *internal.Index
	for _, index = range nodeStatus.Indexes {
		if index.Name == cmd.Index {
			break
		}
	}
	if index == nil {
		return errors.Errorf("Couldn't find index %v in \n%#v", cmd.Index, nodeStatus.Indexes)
	}
	slices := index.Slices
	e := pilosa.NewExecutor()
	e.Holder = pilosa.NewHolder()
	node := &pilosa.Node{
		Host: cmd.Host,
	}
	query, err := pql.ParseString(cmd.Query)
	if err != nil {
		return errors.Wrap(err, "parsing query")
	}

	res, err := e.Exec(ctx, node, cmd.Index, query, slices, nil)
	if err != nil {
		return errors.Wrap(err, "running exec")
	}
	enc := json.NewEncoder(cmd.Stdout)
	err = enc.Encode(res)
	if err != nil {
		return errors.Wrapf(err, "encoding result: %#v", res)
	}
	return nil
}
