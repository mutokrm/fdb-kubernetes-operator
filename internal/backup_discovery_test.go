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
	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"time"
)

var _ = Describe("backup discovery Job", func() {
	DescribeTable(
		"inherits both image layouts and installs the helper for non-root execution",
		func(imageType fdbv1beta2.ImageType) {
			backup := CreateDefaultBackup(CreateDefaultCluster())
			backup.Spec.Version = "7.4.6"
			backup.Spec.ImageType = ptr.To(imageType)
			timestamp := metav1.NewTime(time.Date(2020, 10, 10, 2, 0, 0, 0, time.UTC))
			backup.Spec.Expiration = &fdbv1beta2.BackupExpiration{
				RestorableAfterTimestamp: &timestamp,
			}
			backup.Spec.EncryptionKeyPath = "/keys/backup.key"
			backup.Spec.PodTemplateSpec = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					RunAsUser:    ptr.To(int64(1234)),
					RunAsNonRoot: ptr.To(true),
				},
				Containers: []corev1.Container{
					{
						Name: fdbv1beta2.MainContainerName,
						SecurityContext: &corev1.SecurityContext{
							ReadOnlyRootFilesystem: ptr.To(true),
						},
					},
				},
			}}
			backup.Status.BackupDetails = &fdbv1beta2.FoundationDBBackupStatusBackupDetails{
				URL: "file:///backup",
			}
			backup.Status.Expiration = &fdbv1beta2.BackupExpirationStatus{
				JobName:     "discovery",
				RequestHash: "request",
				Stage:       fdbv1beta2.BackupExpirationStageDiscover,
			}
			job, err := GetBackupDiscoveryJob(backup, "operator:test")
			Expect(err).NotTo(HaveOccurred())
			pod := job.Spec.Template.Spec
			Expect(pod.Containers).To(HaveLen(1))
			Expect(ptr.Deref(job.Spec.BackoffLimit, 1)).To(BeZero())
			Expect(ptr.Deref(pod.AutomountServiceAccountToken, true)).To(BeFalse())
			Expect(pod.SecurityContext.FSGroup).NotTo(BeNil())
			Expect(*pod.SecurityContext.RunAsUser).To(Equal(int64(1234)))
			main := pod.Containers[0]
			Expect(
				main.Args,
			).To(ContainElements("--timestamp", "2020-10-10T02:00:00Z", "--encryption-key-file", "/keys/backup.key", "--request-hash", "request"))
			Expect(main.TerminationMessagePolicy).To(Equal(corev1.TerminationMessageReadFile))
			Expect(
				main.VolumeMounts,
			).To(ContainElement(corev1.VolumeMount{Name: BackupDiscoveryVolume, MountPath: BackupDiscoveryPath, ReadOnly: true}))
			init := pod.InitContainers[len(pod.InitContainers)-1]
			Expect(init.Image).To(Equal("operator:test"))
			Expect(init.Args).To(Equal([]string{"--install", BackupDiscoveryExecutable}))
			Expect(*init.SecurityContext.ReadOnlyRootFilesystem).To(BeTrue())
			backup.Spec.PodTemplateSpec.Spec.Volumes = append(
				backup.Spec.PodTemplateSpec.Spec.Volumes,
				corev1.Volume{Name: BackupDiscoveryVolume},
			)
			_, err = GetBackupDiscoveryJob(backup, "operator:test")
			Expect(err).To(HaveOccurred())
		},
		Entry("split", fdbv1beta2.ImageTypeSplit),
		Entry("unified", fdbv1beta2.ImageTypeUnified),
	)
})
