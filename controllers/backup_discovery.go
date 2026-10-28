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

package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	"github.com/FoundationDB/fdb-kubernetes-operator/v2/internal"
	"github.com/FoundationDB/fdb-kubernetes-operator/v2/internal/backupdiscovery"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// backupDiscoveryIdentity separates a durable recovery point from an execution attempt.
func (r *FoundationDBBackupReconciler) backupDiscoveryIdentity(
	ctx context.Context,
	backup *fdbv1beta2.FoundationDBBackup,
) (string, string, error) {
	cluster := &fdbv1beta2.FoundationDBCluster{}
	if err := r.Get(
		ctx,
		client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.ClusterName},
		cluster,
	); err != nil {
		return "", "", err
	}
	if backup.Status.BackupDetails == nil || backup.Status.BackupDetails.URL == "" {
		return "", "", errors.New("waiting for backup destination")
	}
	identityData, err := json.Marshal(
		[]string{
			string(backup.UID),
			string(cluster.UID),
			backup.Status.BackupDetails.URL,
			backup.Spec.Expiration.RestorableAfterTimestamp.UTC().Format(time.RFC3339),
		},
	)
	if err != nil {
		return "", "", err
	}
	identity := fmt.Sprintf("%x", sha256.Sum256(identityData))
	requestData, err := json.Marshal(struct {
		Identity, Image string
		Generation      int64
	}{identity, r.BackupDiscoveryHelperImage, backup.Generation})
	if err != nil {
		return "", "", err
	}
	return identity, fmt.Sprintf("%x", sha256.Sum256(requestData)), nil
}

func (r *FoundationDBBackupReconciler) reconcileAutomaticBackupExpiration(
	ctx context.Context,
	backup *fdbv1beta2.FoundationDBBackup,
	job *batchv1.Job,
) *requeue {
	pending := &requeue{
		delay:          r.backupExpirationPollInterval(),
		delayedRequeue: true,
		message:        "waiting for automatic backup expiration",
	}
	fail := func(err error) *requeue { return &requeue{curError: err} }
	status := backup.Status.Expiration
	automatic := backup.Spec.Expiration != nil &&
		backup.Spec.Expiration.RestorableAfterTimestamp != nil
	if job != nil {
		if status.JobUID == "" {
			status.JobUID = string(job.UID)
			if err := r.updateOrApply(ctx, backup); err != nil {
				return fail(err)
			}
			return pending
		}
		if status.JobUID != string(job.UID) {
			return fail(errors.New("retention Job UID changed"))
		}
		// Created expiration Jobs retain their immutable arguments after policy changes.
		if status.Stage == fdbv1beta2.BackupExpirationStageExpire {
			return r.reconcileBackupExpirationJob(ctx, backup, job)
		}
		if err := r.reconcileBackupDiscoveryJob(ctx, backup, job); err != nil {
			return fail(err)
		}
		return pending
	}
	if !automatic {
		if backup.Spec.Expiration == nil {
			backup.Status.Expiration = nil
		} else if status != nil {
			status.Stage = ""
			status.JobUID = ""
			status.RequestHash = ""
			status.RequestGeneration = 0
			status.RecoveryPoint = nil
			status.PreparedExpiration = nil
			if status.Phase == fdbv1beta2.BackupExpirationPhaseRunning {
				status.Phase = fdbv1beta2.BackupExpirationPhaseFailed
			}
		}
		if err := r.updateOrApply(ctx, backup); err != nil {
			return fail(err)
		}
		return pending
	}
	if req := requeueUntilBackupExpiration(status); req != nil {
		return req
	}
	identity, requestHash, err := r.backupDiscoveryIdentity(ctx, backup)
	if err != nil {
		return fail(err)
	}
	if status != nil && status.Phase == fdbv1beta2.BackupExpirationPhaseRunning &&
		status.JobUID != "" {
		if err = r.failBackupDiscovery(
			ctx,
			backup,
			"observed retention Job disappeared before completion",
		); err != nil {
			return fail(err)
		}
		return pending
	}
	sameRequest := status != nil && status.RequestHash == requestHash &&
		status.RequestGeneration == backup.Generation
	if status == nil || status.Phase != fdbv1beta2.BackupExpirationPhaseRunning || !sameRequest {
		stage := fdbv1beta2.BackupExpirationStageDiscover
		var point *fdbv1beta2.BackupRecoveryPoint
		var prepared *fdbv1beta2.PreparedBackupExpiration
		failures := 0
		if status != nil {
			failures = status.FailureCount
			if status.RecoveryPoint != nil && status.RecoveryPoint.Identity == identity {
				point = status.RecoveryPoint
			}
			if sameRequest && point != nil &&
				status.Stage == fdbv1beta2.BackupExpirationStageDiscover &&
				status.Phase == fdbv1beta2.BackupExpirationPhaseSucceeded &&
				status.PreparedExpiration != nil {
				stage = fdbv1beta2.BackupExpirationStageExpire
				prepared = status.PreparedExpiration
			}
		}
		nameHash := sha256.Sum256(
			[]byte(string(backup.UID) + "/" + time.Now().UTC().Format(time.RFC3339Nano)),
		)
		backup.Status.Expiration = &fdbv1beta2.BackupExpirationStatus{
			Stage:              stage,
			JobName:            fmt.Sprintf("fdbbackup-expire-%x", nameHash[:16]),
			Phase:              fdbv1beta2.BackupExpirationPhaseRunning,
			RequestHash:        requestHash,
			RequestGeneration:  backup.Generation,
			RecoveryPoint:      point,
			PreparedExpiration: prepared,
			FailureCount:       failures,
		}
		if err = r.updateOrApply(ctx, backup); err != nil {
			return fail(err)
		}
		return pending
	}
	if status.Stage == fdbv1beta2.BackupExpirationStageDiscover {
		job, err = internal.GetBackupDiscoveryJob(backup, r.BackupDiscoveryHelperImage)
	} else {
		job, err = internal.GetBackupExpirationJob(backup, time.Now())
	}
	if err != nil {
		if err = r.failBackupDiscovery(ctx, backup, err.Error()); err != nil {
			return fail(err)
		}
		return pending
	}
	if err = r.Create(ctx, job); err != nil {
		if k8serrors.IsAlreadyExists(err) {
			return pending
		}
		return fail(err)
	}
	status.JobUID = string(job.UID)
	if err = r.updateOrApply(ctx, backup); err != nil {
		return fail(err)
	}
	return pending
}

// reconcileBackupDiscoveryJob persists the result before allowing foreground cleanup.
func (r *FoundationDBBackupReconciler) reconcileBackupDiscoveryJob(
	ctx context.Context,
	backup *fdbv1beta2.FoundationDBBackup,
	job *batchv1.Job,
) error {
	phase, message := backupExpirationJobResult(job)
	if phase == fdbv1beta2.BackupExpirationPhaseRunning {
		return nil
	}
	if backup.Status.Expiration.Phase != fdbv1beta2.BackupExpirationPhaseRunning {
		return r.deleteBackupExpirationJob(ctx, job)
	}
	if phase == fdbv1beta2.BackupExpirationPhaseFailed {
		if _, err := r.readBackupDiscoveryResult(ctx, backup, job); err != nil {
			message += ": " + err.Error()
		}
		return r.failBackupDiscovery(ctx, backup, message)
	}
	if backup.Spec.Expiration == nil || backup.Spec.Expiration.RestorableAfterTimestamp == nil {
		return r.failBackupDiscovery(ctx, backup, "timestamp retention disabled; result discarded")
	}
	identity, requestHash, err := r.backupDiscoveryIdentity(ctx, backup)
	if err != nil {
		return err
	}
	status := backup.Status.Expiration
	if status.RequestGeneration != backup.Generation || requestHash != status.RequestHash {
		return r.failBackupDiscovery(ctx, backup, "discovery inputs changed; result discarded")
	}
	result, err := r.readBackupDiscoveryResult(ctx, backup, job)
	if err != nil {
		return r.failBackupDiscovery(ctx, backup, err.Error())
	}
	status.RecoveryPoint = &fdbv1beta2.BackupRecoveryPoint{
		Timestamp: *backup.Spec.Expiration.RestorableAfterTimestamp,
		Identity:  identity,
		Version:   result.ProtectedVersion,
	}
	status.PreparedExpiration = &fdbv1beta2.PreparedBackupExpiration{
		CutoffVersion:      result.CutoffVersion,
		SnapshotEndVersion: result.SnapshotEndVersion,
	}
	status.Phase = fdbv1beta2.BackupExpirationPhaseSucceeded
	now := metav1.Now()
	status.CompletionTime = &now
	// Discovery is intermediate: keep the failure count and do not schedule the next interval.
	return r.updateOrApply(ctx, backup)
}

// failBackupDiscovery shares the expiration retry schedule and preserves the accepted checkpoint.
func (r *FoundationDBBackupReconciler) failBackupDiscovery(
	ctx context.Context,
	backup *fdbv1beta2.FoundationDBBackup,
	message string,
) error {
	backup.Status.Expiration.PreparedExpiration = nil
	return r.setBackupExpirationStatus(ctx, backup, fdbv1beta2.BackupExpirationPhaseFailed, message)
}

func (r *FoundationDBBackupReconciler) readBackupDiscoveryResult(
	ctx context.Context,
	backup *fdbv1beta2.FoundationDBBackup,
	job *batchv1.Job,
) (backupdiscovery.Result, error) {
	var result backupdiscovery.Result
	pods := &corev1.PodList{}
	if err := r.List(
		ctx,
		pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{"batch.kubernetes.io/controller-uid": string(job.UID)},
	); err != nil {
		return result, err
	}
	count := 0
	for _, pod := range pods.Items {
		if !metav1.IsControlledBy(&pod, job) ||
			(pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed) {
			continue
		}
		for _, container := range pod.Status.ContainerStatuses {
			if container.Name != fdbv1beta2.MainContainerName ||
				container.State.Terminated == nil {
				continue
			}
			count++
			var err error
			result, err = backupdiscovery.DecodeResult([]byte(container.State.Terminated.Message))
			if err != nil {
				return result, err
			}
			if (pod.Status.Phase != corev1.PodSucceeded || container.State.Terminated.ExitCode != 0) &&
				result.Error == "" {
				return result, errors.New("discovery container failed without a diagnostic")
			}
		}
	}
	if count != 1 {
		return result, errors.New("discovery requires exactly one completed owned Pod result")
	}
	pinned := int64(0)
	if backup.Status.Expiration.RecoveryPoint != nil {
		pinned = backup.Status.Expiration.RecoveryPoint.Version
	}
	return result, result.Validate(backup.Status.Expiration.RequestHash, pinned)
}
