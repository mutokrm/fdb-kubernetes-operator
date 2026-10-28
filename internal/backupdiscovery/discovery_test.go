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

package backupdiscovery

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

const fixture = `{"SchemaVersion":"1.0.0","URL":"file:///backup","Restorable":true,"FileLevelEncryption":true,"MutationLogType":"default","MinLogBegin":{"Version":100},"ContiguousLogEnd":{"Version":500},"Snapshots":[{"Start":{"Version":100},"End":{"Version":150},"Restorable":true},{"Start":{"Version":200},"End":{"Version":250},"Restorable":true},{"Start":{"Version":280},"End":{"Version":350},"Restorable":true}]}`

func TestSelectCutoff(t *testing.T) {
	cutoff, end, err := SelectCutoff(fixture, "file:///backup", 300, true)
	if err != nil || cutoff != 200 || end != 250 {
		t.Fatalf("unexpected candidate: %d %d %v", cutoff, end, err)
	}
	for name, data := range map[string]string{
		"gap":               strings.Replace(fixture, `"Version":500`, `"Version":275`, 1),
		"exclusive end":     strings.Replace(fixture, `"Version":500`, `"Version":300`, 1),
		"partitioned":       strings.Replace(fixture, `"default"`, `"partitioned"`, 1),
		"missing log start": strings.Replace(fixture, `"MinLogBegin"`, `"Unknown"`, 1),
		"missing snapshots": strings.Replace(fixture, `"Snapshots"`, `"Unknown"`, 1),
		"unrestorable":      strings.Replace(fixture, `"Restorable":true`, `"Restorable":false`, 1),
		"wrong destination": strings.Replace(fixture, `file:///backup`, `file:///other`, 1),
		"reversed snapshot": strings.Replace(fixture, `"Version":250`, `"Version":190`, 1),
		"error response":    strings.Replace(fixture, `"SchemaVersion"`, `"error":"failed","SchemaVersion"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := SelectCutoff(data, "file:///backup", 300, true); err == nil {
				t.Fatal("unsafe description accepted")
			}
		})
	}
	if _, _, err = SelectCutoff(fixture, "file:///backup", 300, false); err == nil {
		t.Fatal("missing key accepted")
	}
	if _, _, err = SelectCutoff(
		strings.Replace(fixture, `"FileLevelEncryption":true`, `"FileLevelEncryption":false`, 1),
		"file:///backup",
		300,
		false,
	); err != nil {
		t.Fatal(err)
	}
}

func TestParseRestore(t *testing.T) {
	timestamp := "2020/10/10.02:00:00+0000"
	output := "Timestamp '" + timestamp + "' resolves to version 300\nBackup can be used to restore to version 300\n"
	if v, err := ParseRestore(output, timestamp, 0); err != nil || v != 300 {
		t.Fatalf("%d %v", v, err)
	}
	for _, bad := range []string{"", output + output, output + "ERROR: bad key\n", strings.Replace(output, "resolves to version 300", "resolves to version 301", 1), strings.Replace(output, timestamp, "different", 1)} {
		if _, err := ParseRestore(bad, timestamp, 0); err == nil {
			t.Fatal("ambiguous output accepted")
		}
	}
	if _, err := ParseRestore(
		"Backup can be used to restore to version 301\n",
		"",
		300,
	); err == nil {
		t.Fatal("changed pin accepted")
	}
}

func TestDiscoveryCommands(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		o := Options{
			Destination:       "file:///backup",
			ClusterFile:       "/cluster",
			EncryptionKeyFile: "/key",
			RequestHash:       "request",
			Knobs:             []string{"--knob_http_request_timeout=10"},
		}
		target := "--timestamp 2020/10/10.02:00:00+0000 --orig_cluster_file /cluster"
		firstOutput := "Timestamp '2020/10/10.02:00:00+0000' resolves to version 300\n"
		o.Timestamp = "2020-10-10T10:00:00+08:00"
		if pinned {
			o.Timestamp, o.Version = "", 300
			target, firstOutput = "-v 300", ""
		}
		const restore = "fdbrestore start --dryrun -r file:///backup --encryption-key-file /key "
		commands := []string{
			restore + target,
			"fdbbackup describe -d file:///backup --json --deep",
			restore + "-v 300",
		}
		outputs := []string{
			firstOutput + "Backup can be used to restore to version 300\n",
			fixture,
			"Backup can be used to restore to version 300\n",
		}
		// Each command failure must stop the sequence; zero exercises the success path.
		for failAt := 0; failAt <= len(commands); failAt++ {
			calls := 0
			run := func(_ context.Context, binary string, args ...string) (string, error) {
				if calls >= len(commands) {
					t.Fatal("unexpected command")
				}
				if !reflect.DeepEqual(
					append([]string{binary}, args...),
					append(strings.Fields(commands[calls]), o.Knobs...),
				) {
					t.Fatalf("unexpected command: %s %v", binary, args)
				}
				calls++
				if calls == failAt {
					return "", errors.New("failure")
				}
				return outputs[calls-1], nil
			}
			result, err := Discover(context.Background(), o, run)
			if failAt > 0 {
				if err == nil || calls != failAt {
					t.Fatalf("failure did not stop discovery: %v", err)
				}
				continue
			}
			if err != nil || calls != 3 || result.ProtectedVersion != 300 ||
				result.CutoffVersion != 200 {
				t.Fatalf("unexpected result: %+v %v", result, err)
			}
		}
	}
}

func TestResultProtocol(t *testing.T) {
	result := Result{
		ProtocolVersion:    1,
		RequestHash:        "request",
		ProtectedVersion:   300,
		CutoffVersion:      200,
		SnapshotEndVersion: 250,
	}
	data, _ := json.Marshal(result)
	decoded, err := DecodeResult(data)
	if err != nil || decoded != result {
		t.Fatal(err)
	}
	if err = result.Validate("request", 300); err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{string(data) + "{}", strings.Repeat("x", MaxResultBytes+1), `{"unexpected":true}`, `{`} {
		if _, err := DecodeResult([]byte(data)); err == nil {
			t.Fatal("invalid result accepted")
		}
	}
	if result.Validate("other", 300) == nil || result.Validate("request", 301) == nil {
		t.Fatal("stale result accepted")
	}
	result.Error = "no snapshot covers target"
	if err = result.Validate("request", 300); err == nil || err.Error() != result.Error {
		t.Fatalf("diagnostic lost: %v", err)
	}
	if err = result.Validate("other", 300); err == nil || err.Error() == result.Error {
		t.Fatal("unbound diagnostic accepted")
	}
	result.Error = ""
	result.CutoffVersion = 301
	if result.Validate("request", 300) == nil {
		t.Fatal("unsafe boundary accepted")
	}
}

func TestRunCommandBounds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := RunCommand(ctx, "sleep", "10"); err == nil {
		t.Fatal("deadline ignored")
	}
	if _, err := RunCommand(
		context.Background(),
		"sh",
		"-c",
		"yes secret | head -c 9000000",
	); err == nil ||
		strings.Contains(err.Error(), "secret") {
		t.Fatalf("output limit/redaction: %v", err)
	}
	if _, err := RunCommand(
		context.Background(),
		"sh",
		"-c",
		"echo secret; exit 1",
	); err == nil ||
		strings.Contains(err.Error(), "secret") {
		t.Fatalf("failure/redaction: %v", err)
	}
}
