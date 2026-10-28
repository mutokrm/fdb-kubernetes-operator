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
	"encoding/json"
	"time"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	"github.com/FoundationDB/fdb-kubernetes-operator/v2/internal"
	"github.com/FoundationDB/fdb-kubernetes-operator/v2/internal/backupdiscovery"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("automatic backup expiration", func() {
	var backup *fdbv1beta2.FoundationDBBackup
	ctx := context.Background()
	BeforeEach(func() {
		cluster := internal.CreateDefaultCluster()
		backup = internal.CreateDefaultBackup(cluster)
		timestamp := metav1.NewTime(time.Date(2020, 10, 10, 2, 0, 0, 0, time.UTC))
		backup.Spec.Expiration = &fdbv1beta2.BackupExpiration{RestorableAfterTimestamp: &timestamp}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())
		backup.Status.BackupDetails = &fdbv1beta2.FoundationDBBackupStatusBackupDetails{
			URL: "file:///backup",
		}
		Expect(k8sClient.Status().Update(ctx, backup)).To(Succeed())
		backupReconciler.BackupDiscoveryHelperImage = "operator:test"
	})
	step := func() {
		req := expireBackup{}.reconcile(ctx, backupReconciler, backup)
		if req != nil {
			Expect(req.curError).NotTo(HaveOccurred())
		}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(backup), backup)).To(Succeed())
	}
	jobs := func() []batchv1.Job {
		list := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace(backup.Namespace))).To(Succeed())
		return list.Items
	}
	start := func() batchv1.Job {
		step()
		step()
		Expect(jobs()).To(HaveLen(1))
		return jobs()[0]
	}
	finish := func(job *batchv1.Job, result backupdiscovery.Result) {
		data, err := json.Marshal(result)
		Expect(err).NotTo(HaveOccurred())
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-pod", Namespace: job.Namespace},
		}
		pod.Labels = map[string]string{"batch.kubernetes.io/controller-uid": string(job.UID)}
		owner := metav1.NewControllerRef(job, batchv1.SchemeGroupVersion.WithKind("Job"))
		pod.OwnerReferences = []metav1.OwnerReference{*owner}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.Phase = corev1.PodSucceeded
		terminated := &corev1.ContainerStateTerminated{Message: string(data)}
		condition := batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}
		if result.Error != "" {
			pod.Status.Phase = corev1.PodFailed
			terminated.ExitCode = 1
			condition.Type, condition.Reason = batchv1.JobFailed, "BackoffLimitExceeded"
		}
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{
			{Name: fdbv1beta2.MainContainerName},
		}
		pod.Status.ContainerStatuses[0].State.Terminated = terminated
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		job.Status.Conditions = []batchv1.JobCondition{condition}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	result := func() backupdiscovery.Result {
		return backupdiscovery.Result{
			ProtocolVersion:    1,
			RequestHash:        backup.Status.Expiration.RequestHash,
			ProtectedVersion:   300,
			CutoffVersion:      200,
			SnapshotEndVersion: 250,
		}
	}
	due := func() {
		past := metav1.NewTime(time.Now().Add(-time.Hour))
		backup.Status.Expiration.NextRunTime = &past
		Expect(k8sClient.Status().Update(ctx, backup)).To(Succeed())
	}

	It(
		"persists the checkpoint before cleanup and guarded expiration",
		func() {
			job := start()
			backup.Status.Expiration.FailureCount = 2
			Expect(k8sClient.Status().Update(ctx, backup)).To(Succeed())
			job.Finalizers = []string{metav1.FinalizerDeleteDependents}
			Expect(k8sClient.Update(ctx, &job)).To(Succeed())
			finish(&job, result())
			step()
			Expect(backup.Status.Expiration.RecoveryPoint.Version).To(Equal(int64(300)))
			Expect(backup.Status.Expiration.PreparedExpiration.CutoffVersion).To(Equal(int64(200)))
			Expect(backup.Status.Expiration.FailureCount).To(Equal(2))
			Expect(backup.Status.Expiration.NextRunTime).To(BeNil())
			Expect(jobs()).To(HaveLen(1))
			Expect(jobs()[0].DeletionTimestamp).To(BeNil())
			step()
			step()
			Expect(jobs()).To(HaveLen(1))
			job = jobs()[0]
			Expect(job.DeletionTimestamp).NotTo(BeNil())
			job.Finalizers = nil
			Expect(k8sClient.Update(ctx, &job)).To(Succeed())
			step()
			Expect(jobs()).To(BeEmpty())
			Expect(backup.Status.Expiration.Stage).To(Equal(fdbv1beta2.BackupExpirationStageExpire))
			step()
			Expect(jobs()).To(HaveLen(1))
			job = jobs()[0]
			Expect(
				job.Spec.Template.Spec.Containers[0].Args,
			).To(Equal([]string{"expire", "-d", "file:///backup", "--expire-before-version", "200", "--restorable-after-version", "300"}))
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			}
			Expect(k8sClient.Status().Update(ctx, &job)).To(Succeed())
			step()
			Expect(backup.Status.Expiration.FailureCount).To(BeZero())
			Expect(backup.Status.Expiration.NextRunTime).NotTo(BeNil())
			due()
			step()
			step()
			step()
			Expect(jobs()).To(HaveLen(1))
			Expect(
				jobs()[0].Spec.Template.Spec.Containers[0].Args,
			).To(ContainElements("--version", "300"))
			Expect(
				jobs()[0].Spec.Template.Spec.Containers[0].Args,
			).NotTo(ContainElement("--timestamp"))
		},
	)

	It("rejects stale results and preserves exponential backoff", func() {
		job := start()
		bad := result()
		bad.RequestHash = "stale"
		finish(&job, bad)
		step()
		Expect(backup.Status.Expiration.Phase).To(Equal(fdbv1beta2.BackupExpirationPhaseFailed))
		Expect(backup.Status.Expiration.RecoveryPoint).To(BeNil())
		Expect(backup.Status.Expiration.FailureCount).To(Equal(1))
		step()
		step()
		Expect(jobs()).To(BeEmpty())
		due()
		step()
		step()
		job = jobs()[0]
		bad = result()
		bad.ProtectedVersion = 100
		finish(&job, bad)
		step()
		Expect(backup.Status.Expiration.FailureCount).To(Equal(2))
		Expect(
			time.Until(backup.Status.Expiration.NextRunTime.Time),
		).To(BeNumerically(">", time.Hour))
	})

	It(
		"backs off when an observed Job disappears",
		func() {
			job := start()
			Expect(k8sClient.Delete(ctx, &job)).To(Succeed())
			step()
			Expect(backup.Status.Expiration.Phase).To(Equal(fdbv1beta2.BackupExpirationPhaseFailed))
			Expect(backup.Status.Expiration.FailureCount).To(Equal(1))
			step()
			Expect(jobs()).To(BeEmpty())
		},
	)

	It("discards accepted preparation when execution inputs change", func() {
		job := start()
		finish(&job, result())
		step()
		step()
		backup.Spec.Version = "7.3.69"
		backup.Spec.Expiration.IntervalSeconds = ptr.To(int64(7200))
		Expect(k8sClient.Update(ctx, backup)).To(Succeed())
		step()
		step()
		Expect(jobs()).To(HaveLen(1))
		Expect(backup.Status.Expiration.Stage).To(Equal(fdbv1beta2.BackupExpirationStageDiscover))
		Expect(backup.Status.Expiration.PreparedExpiration).To(BeNil())
		Expect(
			jobs()[0].Spec.Template.Spec.Containers[0].Args,
		).To(ContainElements("--version", "300"))
	})

	It("does not reuse a checkpoint after its destination changes", func() {
		job := start()
		finish(&job, result())
		step()
		step()
		backup.Status.BackupDetails.URL = "file:///other"
		Expect(k8sClient.Status().Update(ctx, backup)).To(Succeed())
		step()
		step()
		Expect(backup.Status.Expiration.RecoveryPoint).To(BeNil())
		Expect(jobs()[0].Spec.Template.Spec.Containers[0].Args).To(ContainElement("--timestamp"))
	})

	It("removes a reserved request when automatic expiration is disabled", func() {
		step()
		backup.Spec.Expiration = nil
		Expect(k8sClient.Update(ctx, backup)).To(Succeed())
		step()
		Expect(jobs()).To(BeEmpty())
		Expect(backup.Status.Expiration).To(BeNil())
	})

	It("fails missing helper configuration through the persisted retry schedule", func() {
		backupReconciler.BackupDiscoveryHelperImage = ""
		step()
		step()
		Expect(jobs()).To(BeEmpty())
		Expect(backup.Status.Expiration.Phase).To(Equal(fdbv1beta2.BackupExpirationPhaseFailed))
		Expect(backup.Status.Expiration.Message).To(ContainSubstring("helper image"))
	})

	DescribeTable("rejects successful Jobs without an unambiguous owned result", func(kind string) {
		job := start()
		finish(&job, result())
		pods := &corev1.PodList{}
		Expect(k8sClient.List(ctx, pods, client.InNamespace(backup.Namespace))).To(Succeed())
		Expect(pods.Items).To(HaveLen(1))
		pod := &pods.Items[0]
		switch kind {
		case "missing":
			Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
		case "foreign":
			pod.OwnerReferences[0].UID = "foreign-job"
			Expect(k8sClient.Update(ctx, pod)).To(Succeed())
		case "failed main":
			pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		case "duplicate":
			other := pod.DeepCopy()
			other.Name += "-duplicate"
			other.UID = ""
			other.ResourceVersion = ""
			Expect(k8sClient.Create(ctx, other)).To(Succeed())
			other.Status = pod.Status
			Expect(k8sClient.Status().Update(ctx, other)).To(Succeed())
		}
		step()
		Expect(backup.Status.Expiration.Phase).To(Equal(fdbv1beta2.BackupExpirationPhaseFailed))
		Expect(backup.Status.Expiration.PreparedExpiration).To(BeNil())
		Expect(backup.Status.Expiration.RecoveryPoint).To(BeNil())
	}, Entry("missing Pod", "missing"), Entry("foreign owner", "foreign"), Entry("failed main", "failed main"), Entry("multiple results", "duplicate"))

	It("discards in-flight discovery after a timestamp edit", func() {
		job := start()
		finish(&job, result())
		timestamp := metav1.NewTime(backup.Spec.Expiration.RestorableAfterTimestamp.Add(time.Hour))
		backup.Spec.Expiration.RestorableAfterTimestamp = &timestamp
		Expect(k8sClient.Update(ctx, backup)).To(Succeed())
		step()
		Expect(backup.Status.Expiration.Phase).To(Equal(fdbv1beta2.BackupExpirationPhaseFailed))
		Expect(backup.Status.Expiration.RecoveryPoint).To(BeNil())
	})

	It("binds execution to persisted generation rather than in-memory defaults", func() {
		_, original, err := backupReconciler.backupDiscoveryIdentity(ctx, backup)
		Expect(err).NotTo(HaveOccurred())
		_, err = internal.GetBackupDeployment(backup)
		Expect(err).NotTo(HaveOccurred())
		_, normalized, err := backupReconciler.backupDiscoveryIdentity(ctx, backup)
		Expect(err).NotTo(HaveOccurred())
		Expect(normalized).To(Equal(original))
		backup.Generation++
		_, changed, err := backupReconciler.backupDiscoveryIdentity(ctx, backup)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).NotTo(Equal(original))
	})

	It("persists helper diagnostics before deleting a failed Job", func() {
		job := start()
		diagnostic := result()
		diagnostic.Error = "no snapshot covers target"
		finish(&job, diagnostic)
		step()
		Expect(backup.Status.Expiration.Message).To(ContainSubstring(diagnostic.Error))
		Expect(backup.Status.Expiration.FailureCount).To(Equal(1))
		Expect(jobs()).To(HaveLen(1))
		step()
		Expect(jobs()).To(BeEmpty())
		Expect(backup.Status.Expiration.Message).To(ContainSubstring(diagnostic.Error))
	})

})
