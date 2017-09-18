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

package cmd

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var Iq *ctl.IQCommand

func NewIQCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Iq = ctl.NewIQCommand(os.Stdin, os.Stdout, os.Stderr)
	iqCmd := &cobra.Command{
		Use:   "iq",
		Short: "Launch internal queries against Pilosa.",
		Long:  `TODO`,

		RunE: func(cmd *cobra.Command, args []string) error {
			if err := Iq.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}

	// Attach flags to the command.
	flags := iqCmd.Flags()
	flags.StringVarP(&Iq.Host, "pilosa-host", "p", "localhost:10101", "Pilosa host to query")
	flags.StringVarP(&Iq.Index, "index", "i", "idx", "Pilosa index to query")
	flags.StringVarP(&Iq.Query, "query", "q", "Bitmap(frame=fram, rowID=1)", "Query to execute.")

	return iqCmd
}

func init() {
	subcommandFns["iq"] = NewIQCommand
}
