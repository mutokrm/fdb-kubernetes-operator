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

// Package backupdiscovery implements non-destructive checkpoint discovery using FDB tools.
package backupdiscovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Protocol and resource bounds for one discovery attempt.
const (
	ProtocolVersion = 1
	TimestampLayout = "2006/01/02.15:04:05-0700"
	MaxResultBytes  = 2048
	MaxOutputBytes  = 8 * 1024 * 1024
	CommandTimeout  = 120 * time.Second
)

// Result contains only validated numeric boundaries and the caller's request identity.
type Result struct {
	// Error is a sanitized diagnostic from a failed discovery command.
	Error              string `json:"error,omitempty"`
	ProtocolVersion    int    `json:"protocolVersion"`
	RequestHash        string `json:"requestHash"`
	ProtectedVersion   int64  `json:"protectedVersion"`
	CutoffVersion      int64  `json:"cutoffVersion"`
	SnapshotEndVersion int64  `json:"snapshotEndVersion"`
}

// Validate verifies the request binding, pinned target and selected boundaries.
func (r Result) Validate(requestHash string, pinned int64) error {
	if r.ProtocolVersion != ProtocolVersion || r.RequestHash == "" || r.RequestHash != requestHash {
		return errors.New("discovery result does not match the request")
	}
	if r.Error != "" {
		return errors.New(r.Error)
	}
	if r.CutoffVersion <= 0 || r.SnapshotEndVersion < r.CutoffVersion ||
		r.ProtectedVersion < r.SnapshotEndVersion ||
		(pinned != 0 && r.ProtectedVersion != pinned) {
		return errors.New("discovery result has invalid boundaries")
	}
	return nil
}

// DecodeResult rejects truncated, oversized or incompatible termination messages.
func DecodeResult(data []byte) (Result, error) {
	var result Result
	if len(data) > MaxResultBytes {
		return result, errors.New("discovery result exceeds limit")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&result); err != nil {
		return result, errors.New("invalid discovery result")
	}
	if d.Decode(new(any)) != io.EOF {
		return result, errors.New("trailing discovery result data")
	}
	return result, nil
}

// Options are immutable inputs for one discovery attempt. Exactly one target is required.
type Options struct {
	Destination       string
	ClusterFile       string
	Timestamp         string
	Version           int64
	EncryptionKeyFile string
	RequestHash       string
	Knobs             []string
}

// Runner executes one FDB command with the supplied context.
type Runner func(context.Context, string, ...string) (string, error)

var successLine = regexp.MustCompile(`(?m)^Backup can be used to restore to version ([0-9]+)\r?$`)
var resolutionLine = regexp.MustCompile(`(?m)^Timestamp '([^']+)' resolves to version ([0-9]+)\r?$`)
var errorLine = regexp.MustCompile(`(?mi)^\s*(ERROR|Fatal Error):`)

// ParseRestore accepts only the explicitly requested checkpoint, never a default/latest target.
func ParseRestore(output, timestamp string, pinned int64) (int64, error) {
	matches := successLine.FindAllStringSubmatch(output, -1)
	if len(matches) != 1 || errorLine.MatchString(output) {
		return 0, errors.New("dry-run did not report unambiguous success")
	}
	version, err := strconv.ParseInt(matches[0][1], 10, 64)
	if err != nil || version <= 0 || (pinned != 0 && version != pinned) {
		return 0, errors.New("dry-run reported an invalid recovery version")
	}
	if timestamp != "" {
		resolved := resolutionLine.FindAllStringSubmatch(output, -1)
		if len(resolved) != 1 || resolved[0][1] != timestamp || resolved[0][2] != matches[0][1] {
			return 0, errors.New("dry-run timestamp resolution mismatch")
		}
	}
	return version, nil
}

type versionPoint struct {
	Version *int64 `json:"Version"`
}

func (p *versionPoint) value() int64 {
	if p == nil || p.Version == nil {
		return 0
	}
	return *p.Version
}

type snapshot struct {
	Start      *versionPoint `json:"Start"`
	End        *versionPoint `json:"End"`
	Restorable *bool         `json:"Restorable"`
}
type description struct {
	SchemaVersion       string          `json:"SchemaVersion"`
	URL                 string          `json:"URL"`
	Error               json.RawMessage `json:"error"`
	Restorable          *bool           `json:"Restorable"`
	FileLevelEncryption *bool           `json:"FileLevelEncryption"`
	Partitioned         *bool           `json:"Partitioned"`
	MutationLogType     *string         `json:"MutationLogType"`
	MinLogBegin         *versionPoint   `json:"MinLogBegin"`
	ContiguousLogEnd    *versionPoint   `json:"ContiguousLogEnd"`
	Snapshots           []snapshot      `json:"Snapshots"`
}

// SelectCutoff conservatively requires one continuous log chain from the snapshot through the target.
func SelectCutoff(data, destination string, target int64, hasKey bool) (int64, int64, error) {
	var d description
	if err := json.Unmarshal([]byte(data), &d); err != nil {
		return 0, 0, errors.New("malformed backup description")
	}
	if d.SchemaVersion != "1.0.0" || d.URL != destination || len(d.Error) != 0 ||
		d.Restorable == nil ||
		!*d.Restorable {
		return 0, 0, errors.New("backup description is unsupported or not restorable")
	}
	// Older FDB versions omit this field; both restore dry-runs still validate readability.
	if d.FileLevelEncryption != nil && *d.FileLevelEncryption && !hasKey {
		return 0, 0, errors.New("encrypted backup discovery requires its key")
	}
	if (d.Partitioned == nil && d.MutationLogType == nil) ||
		(d.Partitioned != nil && *d.Partitioned) ||
		(d.MutationLogType != nil && *d.MutationLogType != "default") {
		return 0, 0, errors.New("automatic retention requires ordinary range-file backups")
	}
	begin, end := d.MinLogBegin.value(), d.ContiguousLogEnd.value()
	if begin <= 0 || target < begin || target >= end {
		return 0, 0, errors.New("target is outside the continuous log chain")
	}
	var cutoff, snapshotEnd int64
	for _, s := range d.Snapshots {
		start, finish := s.Start.value(), s.End.value()
		if start <= 0 || finish < start || s.Restorable == nil {
			return 0, 0, errors.New("invalid snapshot metadata")
		}
		if *s.Restorable && begin <= start && finish <= target &&
			(start > cutoff || (start == cutoff && finish > snapshotEnd)) {
			cutoff, snapshotEnd = start, finish
		}
	}
	if cutoff == 0 {
		return 0, 0, errors.New("no snapshot in the continuous log chain precedes the target")
	}
	return cutoff, snapshotEnd, nil
}

// Discover resolves a target, selects its snapshot and verifies restorability again.
func Discover(ctx context.Context, o Options, run Runner) (Result, error) {
	result := Result{ProtocolVersion: ProtocolVersion, RequestHash: o.RequestHash}
	if o.Destination == "" || o.RequestHash == "" || (o.Timestamp == "") == (o.Version == 0) ||
		o.Version < 0 {
		return result, errors.New("invalid discovery options")
	}
	for _, knob := range o.Knobs {
		if !strings.HasPrefix(knob, "--knob_") {
			return result, errors.New("only FDB knobs may be inherited")
		}
	}
	restore := []string{"start", "--dryrun", "-r", o.Destination}
	if o.EncryptionKeyFile != "" {
		restore = append(restore, "--encryption-key-file", o.EncryptionKeyFile)
	}
	var formatted string
	target := []string{"-v", strconv.FormatInt(o.Version, 10)}
	if o.Timestamp != "" {
		t, err := time.Parse(time.RFC3339, o.Timestamp)
		if err != nil || t.IsZero() || t.After(time.Now()) || t.Nanosecond() != 0 ||
			o.ClusterFile == "" {
			return result, errors.New("invalid recovery timestamp or cluster file")
		}
		formatted = t.UTC().Format(TimestampLayout)
		target = []string{"--timestamp", formatted, "--orig_cluster_file", o.ClusterFile}
	}
	invoke := func(binary string, args []string) (string, error) {
		commandCtx, cancel := context.WithTimeout(ctx, CommandTimeout)
		defer cancel()
		return run(commandCtx, binary, append(args, o.Knobs...)...)
	}
	output, err := invoke("fdbrestore", append(append([]string{}, restore...), target...))
	if err != nil {
		return result, fmt.Errorf("restore dry-run failed: %w", err)
	}
	result.ProtectedVersion, err = ParseRestore(output, formatted, o.Version)
	if err != nil {
		return result, err
	}
	output, err = invoke("fdbbackup", []string{"describe", "-d", o.Destination, "--json", "--deep"})
	if err != nil {
		return result, fmt.Errorf("backup description failed: %w", err)
	}
	result.CutoffVersion, result.SnapshotEndVersion, err = SelectCutoff(
		output,
		o.Destination,
		result.ProtectedVersion,
		o.EncryptionKeyFile != "",
	)
	if err != nil {
		return result, err
	}
	output, err = invoke(
		"fdbrestore",
		append(
			append([]string{}, restore...),
			"-v",
			strconv.FormatInt(result.ProtectedVersion, 10),
		),
	)
	if err != nil {
		return result, fmt.Errorf("final restore dry-run failed: %w", err)
	}
	if _, err = ParseRestore(output, "", result.ProtectedVersion); err != nil {
		return result, err
	}
	return result, result.Validate(o.RequestHash, o.Version)
}

type limitedOutput struct {
	sync.Mutex
	buffer   bytes.Buffer
	cancel   context.CancelFunc
	exceeded bool
}

func (b *limitedOutput) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	if b.buffer.Len()+len(p) > MaxOutputBytes {
		b.exceeded = true
		b.cancel()
		return 0, errors.New("output limit")
	}
	return b.buffer.Write(p)
}

// RunCommand never includes raw output or arguments in an error; URLs may contain credentials.
func RunCommand(ctx context.Context, binary string, args ...string) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	output := &limitedOutput{cancel: cancel}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.WaitDelay = time.Second
	cmd.Stdout, cmd.Stderr = output, output
	err := cmd.Run()
	if output.exceeded {
		return "", errors.New("command output exceeded limit")
	}
	if ctx.Err() != nil {
		return "", errors.New("command cancelled or timed out")
	}
	if err != nil {
		return "", errors.New(
			"FDB command failed; check image, connectivity, credentials and encryption key",
		)
	}
	return output.buffer.String(), nil
}
