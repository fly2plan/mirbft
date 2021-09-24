/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"fmt"
	"io"

	"github.com/fly2plan/mirbft/pkg/statemachine"
)

type NamedLogger struct {
	Level  statemachine.LogLevel
	Name   string
	Output io.Writer
}

func (nl NamedLogger) Log(level statemachine.LogLevel, msg string, args ...interface{}) {
	if level < nl.Level {
		return
	}

	fmt.Fprint(nl.Output, nl.Name)
	fmt.Fprint(nl.Output, ": ")
	fmt.Fprint(nl.Output, msg)
	for i := 0; i < len(args); i++ {
		if i+1 < len(args) {
			switch args[i+1].(type) {
			case []byte:
				fmt.Fprintf(nl.Output, " %s=%x", args[i], args[i+1])
			default:
				fmt.Fprintf(nl.Output, " %s=%v", args[i], args[i+1])
			}
			i++
		} else {
			fmt.Fprintf(nl.Output, " %s=%%MISSING%%", args[i])
		}
	}
	fmt.Fprintf(nl.Output, "\n")
}
