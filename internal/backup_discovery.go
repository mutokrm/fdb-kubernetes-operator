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

package internal

import (
	"errors"
	"strconv"
	"strings"
	"time"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

// Reserved names for the shared discovery executable.
const (
	BackupDiscoveryVolume     = "backup-discovery-helper"
	BackupDiscoveryPath       = "/var/fdb-backup-discovery"
	BackupDiscoveryExecutable = BackupDiscoveryPath + "/backup-discovery"
)

// GetBackupDiscoveryJob reuses the backup runtime while installing a static helper from the operator image.
func GetBackupDiscoveryJob(
	backup *fdbv1beta2.FoundationDBBackup,
	helperImage string,
) (*batchv1.Job, error) {
	status := backup.Status.Expiration
	if helperImage == "" || status == nil || status.RequestHash == "" ||
		backup.Spec.Expiration == nil ||
		backup.Spec.Expiration.RestorableAfterTimestamp == nil {
		return nil, errors.New("timestamp discovery requires a helper image, request and timestamp")
	}
	job, err := getBackupMaintenanceJob(backup)
	if err != nil {
		return nil, err
	}
	pod := &job.Spec.Template.Spec
	for _, volume := range pod.Volumes {
		if volume.Name == BackupDiscoveryVolume {
			return nil, errors.New("backup discovery helper volume conflicts with pod template")
		}
	}
	for _, c := range append(append([]corev1.Container{}, pod.InitContainers...), pod.Containers...) {
		if c.Name == BackupDiscoveryVolume {
			return nil, errors.New("backup discovery init container conflicts with pod template")
		}
		for _, mount := range c.VolumeMounts {
			if mount.MountPath == BackupDiscoveryPath ||
				strings.HasPrefix(mount.MountPath, BackupDiscoveryPath+"/") {
				return nil, errors.New("backup discovery helper path conflicts with pod template")
			}
		}
	}
	// emptyDir needs a writable group for the helper image's default non-root user.
	if pod.SecurityContext == nil {
		pod.SecurityContext = &corev1.PodSecurityContext{}
	}
	if pod.SecurityContext.FSGroup == nil {
		pod.SecurityContext.FSGroup = ptr.To(int64(4059))
	}
	main := &pod.Containers[0]
	main.Command = []string{BackupDiscoveryExecutable}
	main.Args = []string{
		"--destination",
		backup.Status.BackupDetails.URL,
		"--request-hash",
		status.RequestHash,
	}
	if status.RecoveryPoint != nil {
		main.Args = append(
			main.Args,
			"--version",
			strconv.FormatInt(status.RecoveryPoint.Version, 10),
		)
	} else {
		main.Args = append(
			main.Args,
			"--timestamp",
			backup.Spec.Expiration.RestorableAfterTimestamp.UTC().Format(time.RFC3339),
		)
	}
	// Resolve the inherited cluster-file location in the container, including user overrides.
	main.Args = append(main.Args, "--cluster-file", "$("+fdbv1beta2.EnvNameClusterFile+")")
	key, err := backup.GetEncryptionKey()
	if err != nil {
		return nil, err
	}
	if backup.Spec.EncryptionKeyPath != "" && key == "" {
		return nil, errors.New("configured FDB version does not support backup encryption")
	}
	if key != "" {
		main.Args = append(main.Args, "--encryption-key-file", key)
	}
	for _, knob := range backup.Spec.CustomParameters.GetKnobsForBackupRestoreCLI() {
		if strings.HasPrefix(knob, "--knob_") {
			main.Args = append(main.Args, "--knob", knob)
		}
	}
	main.TerminationMessagePath = "/dev/termination-log"
	main.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	main.VolumeMounts = append(
		main.VolumeMounts,
		corev1.VolumeMount{
			Name:      BackupDiscoveryVolume,
			MountPath: BackupDiscoveryPath,
			ReadOnly:  true,
		},
	)
	pod.Volumes = append(
		pod.Volumes,
		corev1.Volume{
			Name:         BackupDiscoveryVolume,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
	)
	// Match the main container's user so non-root operation and inherited fsGroup remain compatible.
	pod.InitContainers = append(pod.InitContainers, corev1.Container{
		Name: BackupDiscoveryVolume, Image: helperImage, ImagePullPolicy: main.ImagePullPolicy,
		Command: []string{
			"/backup-discovery",
		}, Args: []string{"--install", BackupDiscoveryExecutable},
		SecurityContext: main.SecurityContext.DeepCopy(), Resources: *main.Resources.DeepCopy(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: BackupDiscoveryVolume, MountPath: BackupDiscoveryPath},
		},
	})
	pod.AutomountServiceAccountToken = ptr.To(false)
	job.Spec.BackoffLimit = ptr.To(int32(0))
	return job, nil
}
