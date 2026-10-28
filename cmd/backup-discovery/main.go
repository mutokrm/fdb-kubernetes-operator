/*
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2018-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/FoundationDB/fdb-kubernetes-operator/v2/internal/backupdiscovery"
)

type knobs []string

func (k *knobs) String() string { return fmt.Sprint([]string(*k)) }
func (k *knobs) Set(value string) error {
	*k = append(*k, value)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	var o backupdiscovery.Options
	var install, resultPath string
	var inherited knobs
	flag.StringVar(&install, "install", "", "Copy this static helper to the shared volume")
	flag.StringVar(&o.Destination, "destination", "", "Backup container")
	flag.StringVar(&o.ClusterFile, "cluster-file", "", "Source cluster file")
	flag.StringVar(&o.Timestamp, "timestamp", "", "Requested RFC3339 timestamp")
	flag.Int64Var(&o.Version, "version", 0, "Accepted recovery version")
	flag.StringVar(&o.EncryptionKeyFile, "encryption-key-file", "", "Backup encryption key")
	flag.StringVar(&o.RequestHash, "request-hash", "", "Opaque request identity")
	flag.StringVar(&resultPath, "result-file", "/dev/termination-log", "Discovery result path")
	flag.Var(&inherited, "knob", "Inherited --knob_ parameter")
	flag.Parse()
	if install != "" {
		return installHelper(install)
	}
	o.Knobs = inherited
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	result, discoveryErr := backupdiscovery.Discover(ctx, o, backupdiscovery.RunCommand)
	if discoveryErr != nil {
		result.Error = discoveryErr.Error()
	}
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if len(data) > backupdiscovery.MaxResultBytes {
		return fmt.Errorf("result exceeds protocol limit")
	}
	if err = os.WriteFile(resultPath, data, 0600); err != nil {
		return fmt.Errorf("cannot write discovery result")
	}
	fmt.Println(string(data))
	return discoveryErr
}
func installHelper(destination string) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	source, err := os.Open(executable)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.CreateTemp(filepath.Dir(destination), ".discovery-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(target.Name()) }()
	if _, err = io.Copy(target, source); err != nil {
		target.Close()
		return err
	}
	if err = target.Chmod(0555); err != nil {
		target.Close()
		return err
	}
	if err = target.Close(); err != nil {
		return err
	}
	return os.Rename(target.Name(), destination)
}
