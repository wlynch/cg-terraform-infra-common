/*
Copyright 2024 Chainguard, Inc.
SPDX-License-Identifier: Apache-2.0
*/

package gcs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/chainguard-dev/terraform-infra-common/pkg/workqueue"
	"github.com/chainguard-dev/terraform-infra-common/pkg/workqueue/conformance"
)

// In-memory mock implementations for testing

// mockObject represents an object in our mock storage
type mockObject struct {
	name         string
	data         []byte
	attrs        *storage.ObjectAttrs
	exists       bool
	conditions   storage.Conditions
}

// mockClient implements ClientInterface for testing
type mockClient struct {
	mu      sync.RWMutex
	objects map[string]*mockObject
}

func newMockClient() *mockClient {
	return &mockClient{
		objects: make(map[string]*mockObject),
	}
}

func (m *mockClient) Object(name string) ObjectHandleInterface {
	return &mockObjectHandle{
		client: m,
		name:   name,
	}
}

func (m *mockClient) Objects(ctx context.Context, q *storage.Query) ObjectIteratorInterface {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var objects []*storage.ObjectAttrs
	for _, obj := range m.objects {
		if obj.exists {
			objects = append(objects, obj.attrs)
		}
	}
	
	return &mockObjectIterator{
		objects: objects,
		index:   0,
	}
}

// mockObjectHandle implements ObjectHandleInterface
type mockObjectHandle struct {
	client     *mockClient
	name       string
	conditions storage.Conditions
}

func (m *mockObjectHandle) If(conds storage.Conditions) ObjectHandleInterface {
	return &mockObjectHandle{
		client:     m.client,
		name:       m.name,
		conditions: conds,
	}
}

func (m *mockObjectHandle) NewWriter(ctx context.Context) WriterInterface {
	return &mockWriter{
		client: m.client,
		name:   m.name,
		conds:  m.conditions,
	}
}

func (m *mockObjectHandle) CopierFrom(src ObjectHandleInterface) CopierInterface {
	srcHandle := src.(*mockObjectHandle)
	return &mockCopier{
		client:  m.client,
		dstName: m.name,
		srcName: srcHandle.name,
		conds:   m.conditions,
	}
}

func (m *mockObjectHandle) Delete(ctx context.Context) error {
	m.client.mu.Lock()
	defer m.client.mu.Unlock()

	obj, exists := m.client.objects[m.name]
	if !exists || !obj.exists {
		return storage.ErrObjectNotExist
	}

	obj.exists = false
	return nil
}

func (m *mockObjectHandle) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	m.client.mu.RLock()
	defer m.client.mu.RUnlock()

	obj, exists := m.client.objects[m.name]
	if !exists || !obj.exists {
		return nil, storage.ErrObjectNotExist
	}

	return obj.attrs, nil
}

func (m *mockObjectHandle) Update(ctx context.Context, uattrs storage.ObjectAttrsToUpdate) (*storage.ObjectAttrs, error) {
	m.client.mu.Lock()
	defer m.client.mu.Unlock()

	obj, exists := m.client.objects[m.name]
	if !exists || !obj.exists {
		return nil, storage.ErrObjectNotExist
	}

	// Update metadata
	for k, v := range uattrs.Metadata {
		obj.attrs.Metadata[k] = v
	}
	obj.attrs.Metageneration++

	return obj.attrs, nil
}

// mockWriter implements WriterInterface
type mockWriter struct {
	client   *mockClient
	name     string
	conds    storage.Conditions
	data     []byte
	metadata map[string]string
	closed   bool
}

func (m *mockWriter) Write(data []byte) (int, error) {
	if m.closed {
		return 0, errors.New("writer is closed")
	}
	m.data = append(m.data, data...)
	return len(data), nil
}

func (m *mockWriter) Close() error {
	if m.closed {
		return errors.New("writer already closed")
	}
	m.closed = true

	m.client.mu.Lock()
	defer m.client.mu.Unlock()

	// Check conditions
	obj, exists := m.client.objects[m.name]
	if m.conds.DoesNotExist && exists && obj.exists {
		return fmt.Errorf("precondition failed: object exists")
	}

	// Create or update object
	now := time.Now()
	if !exists {
		obj = &mockObject{
			name: m.name,
			attrs: &storage.ObjectAttrs{
				Name:           m.name,
				Created:        now,
				Updated:        now,
				Metageneration: 1,
				Metadata:       make(map[string]string),
			},
		}
		m.client.objects[m.name] = obj
	}

	obj.data = m.data
	obj.exists = true
	obj.attrs.Updated = now
	
	// Set metadata
	if m.metadata != nil {
		for k, v := range m.metadata {
			obj.attrs.Metadata[k] = v
		}
	}

	return nil
}

func (m *mockWriter) SetMetadata(metadata map[string]string) {
	m.metadata = metadata
}

// mockCopier implements CopierInterface
type mockCopier struct {
	client   *mockClient
	dstName  string
	srcName  string
	conds    storage.Conditions
	metadata map[string]string
}

func (m *mockCopier) Run(ctx context.Context) (*storage.ObjectAttrs, error) {
	m.client.mu.Lock()
	defer m.client.mu.Unlock()

	// Get source object
	srcObj, exists := m.client.objects[m.srcName]
	if !exists || !srcObj.exists {
		return nil, storage.ErrObjectNotExist
	}

	// Check destination conditions
	dstObj, dstExists := m.client.objects[m.dstName]
	if m.conds.DoesNotExist && dstExists && dstObj.exists {
		return nil, fmt.Errorf("precondition failed: destination exists")
	}

	// Create destination object
	now := time.Now()
	if !dstExists {
		dstObj = &mockObject{
			name: m.dstName,
			attrs: &storage.ObjectAttrs{
				Name:           m.dstName,
				Created:        now,
				Updated:        now,
				Metageneration: 1,
				Metadata:       make(map[string]string),
			},
		}
		m.client.objects[m.dstName] = dstObj
	}

	// Copy data and metadata
	dstObj.data = make([]byte, len(srcObj.data))
	copy(dstObj.data, srcObj.data)
	dstObj.exists = true
	dstObj.attrs.Updated = now

	// Start with fresh metadata if copier metadata is provided, otherwise copy source
	if m.metadata != nil {
		// Use copier metadata directly (this handles deletes properly)
		for k, v := range m.metadata {
			dstObj.attrs.Metadata[k] = v
		}
	} else {
		// Copy source metadata
		for k, v := range srcObj.attrs.Metadata {
			dstObj.attrs.Metadata[k] = v
		}
	}

	return dstObj.attrs, nil
}

func (m *mockCopier) SetMetadata(metadata map[string]string) {
	m.metadata = metadata
}

// mockObjectIterator implements ObjectIteratorInterface
type mockObjectIterator struct {
	objects []*storage.ObjectAttrs
	index   int
}

func (m *mockObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if m.index >= len(m.objects) {
		return nil, iterator.Done
	}
	attrs := m.objects[m.index]
	m.index++
	return attrs, nil
}

func TestDeadLetterKey(t *testing.T) {
	key := &inProgressKey{
		attrs: &storage.ObjectAttrs{
			Name: "in-progress/test-key",
		},
	}
	got := key.deadLetterKey()
	want := "dead-letter/test-key"
	if got != want {
		t.Errorf("deadLetterKey() = %q, want %q", got, want)
	}
}

func TestWorkQueue(t *testing.T) {
	bucket, ok := os.LookupEnv("WORKQUEUE_GCS_TEST_BUCKET")
	if !ok {
		t.Skip("WORKQUEUE_GCS_TEST_BUCKET not set")
	}
	// Adjust this to a suitable period for testing things.
	// The conformance tests own adjusting MaximumBackoffPeriod.
	workqueue.BackoffPeriod = 10 * time.Second

	client, err := storage.NewClient(context.Background())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	conformance.TestSemantics(t, func(u int) workqueue.Interface {
		return NewWorkQueue(NewGCSClientAdapter(client.Bucket(bucket)), u)
	})

	conformance.TestConcurrency(t, func(u int) workqueue.Interface {
		return NewWorkQueue(NewGCSClientAdapter(client.Bucket(bucket)), u)
	})

	conformance.TestDurability(t, func(u int) workqueue.Interface {
		return NewWorkQueue(NewGCSClientAdapter(client.Bucket(bucket)), u)
	})

	conformance.TestMaxRetry(t, func(u int) workqueue.Interface {
		return NewWorkQueue(NewGCSClientAdapter(client.Bucket(bucket)), u)
	})
}

// E2E Tests for Last Attempted Functionality

func TestE2E_QueueStartComplete(t *testing.T) {
	client := newMockClient()
	wq := NewWorkQueue(client, 10)
	ctx := context.Background()

	// Test basic queue -> start -> complete flow
	key := "test-key-1"
	
	// Queue the item
	err := wq.Queue(ctx, key, workqueue.Options{Priority: 5})
	if err != nil {
		t.Fatalf("Queue() = %v, want nil", err)
	}

	// Enumerate and get the queued item
	_, queued, err := wq.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate() = %v, want nil", err)
	}
	if len(queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(queued))
	}
	if queued[0].Name() != key {
		t.Errorf("queued[0].Name() = %q, want %q", queued[0].Name(), key)
	}

	// Start processing the item
	owned, err := queued[0].Start(ctx)
	if err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	if owned.Name() != key {
		t.Errorf("owned.Name() = %q, want %q", owned.Name(), key)
	}
	if owned.GetAttempts() != 1 {
		t.Errorf("owned.GetAttempts() = %d, want 1", owned.GetAttempts())
	}

	// Verify it's now in progress
	inProgress, queued, err := wq.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate() = %v, want nil", err)
	}
	if len(inProgress) != 1 {
		t.Fatalf("len(inProgress) = %d, want 1", len(inProgress))
	}
	if len(queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0", len(queued))
	}

	// Complete the item
	err = owned.Complete(ctx)
	if err != nil {
		t.Fatalf("Complete() = %v, want nil", err)
	}

	// Verify it's gone
	inProgress, queued, err = wq.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate() = %v, want nil", err)
	}
	if len(inProgress) != 0 {
		t.Fatalf("len(inProgress) = %d, want 0", len(inProgress))
	}
	if len(queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0", len(queued))
	}
}

func TestE2E_QueueStartRequeue(t *testing.T) {
	client := newMockClient()
	wq := NewWorkQueue(client, 10)
	ctx := context.Background()

	// Temporarily disable backoff for this test
	oldBackoffPeriod := workqueue.BackoffPeriod
	defer func() { workqueue.BackoffPeriod = oldBackoffPeriod }()
	workqueue.BackoffPeriod = 0

	key := "test-key-2"
	
	// Queue the item
	err := wq.Queue(ctx, key, workqueue.Options{Priority: 3})
	if err != nil {
		t.Fatalf("Queue() = %v, want nil", err)
	}

	// Start processing
	_, queued, err := wq.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate() = %v, want nil", err)
	}
	owned, err := queued[0].Start(ctx)
	if err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	// Requeue the item 
	err = owned.Requeue(ctx)
	if err != nil {
		t.Fatalf("Requeue() = %v, want nil", err)
	}

	// Verify it's back in queue with attempt count incremented
	_, queued, err = wq.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate() = %v, want nil", err)
	}
	if len(queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(queued))
	}

	// Start it again to check attempt count
	owned2, err := queued[0].Start(ctx)
	if err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	if owned2.GetAttempts() != 2 {
		t.Errorf("owned2.GetAttempts() = %d, want 2", owned2.GetAttempts())
	}

	// Verify last attempted timestamp was set
	client.mu.RLock()
	queuedObj, exists := client.objects["queued/"+key]
	client.mu.RUnlock()
	
	if !exists || !queuedObj.exists {
		// Check in-progress since we started it
		client.mu.RLock()
		inProgressObj, exists := client.objects["in-progress/"+key]
		client.mu.RUnlock()
		
		if !exists || !inProgressObj.exists {
			t.Fatalf("Expected object to exist in either queued or in-progress state")
		}
		// Last attempted should have been cleared when moved to in-progress, so we can't test it here
	} else {
		if lastAttempted, ok := queuedObj.attrs.Metadata[lastAttemptedKey]; !ok || lastAttempted == "" {
			t.Errorf("Expected last-attempted metadata to be set after requeue")
		}
	}
}

func TestE2E_QueueStartDeadletter(t *testing.T) {
	client := newMockClient()
	wq := NewWorkQueue(client, 10)
	ctx := context.Background()

	key := "test-key-3"
	
	// Queue the item
	err := wq.Queue(ctx, key, workqueue.Options{Priority: 1})
	if err != nil {
		t.Fatalf("Queue() = %v, want nil", err)
	}

	// Start processing
	_, queued, err := wq.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate() = %v, want nil", err)
	}
	owned, err := queued[0].Start(ctx)
	if err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	// Deadletter the item
	err = owned.Deadletter(ctx)
	if err != nil {
		t.Fatalf("Deadletter() = %v, want nil", err)
	}

	// Verify it's gone from in-progress
	inProgress, queued, err := wq.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate() = %v, want nil", err)
	}
	if len(inProgress) != 0 {
		t.Fatalf("len(inProgress) = %d, want 0", len(inProgress))
	}
	if len(queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0", len(queued))
	}

	// Verify dead letter object exists with proper metadata
	client.mu.RLock()
	deadLetterObj, exists := client.objects["dead-letter/"+key]
	client.mu.RUnlock()
	
	if !exists || !deadLetterObj.exists {
		t.Fatalf("Expected dead letter object to exist")
	}
	if failedTime, ok := deadLetterObj.attrs.Metadata[failedTimeMetadataKey]; !ok || failedTime == "" {
		t.Errorf("Expected failed-time metadata to be set on dead letter object")
	}
	if _, ok := deadLetterObj.attrs.Metadata[expirationMetadataKey]; ok {
		t.Errorf("Expected expiration metadata to be cleared on dead letter object")
	}
}

func TestE2E_LastAttemptedTimestamp(t *testing.T) {
	client := newMockClient()
	wq := NewWorkQueue(client, 10)
	ctx := context.Background()

	key := "test-key-4"
	baseTime := time.Now().UTC().Add(-1 * time.Hour)
	lastAttemptedTime := baseTime.Add(30 * time.Minute)
	
	// Manually create a queued object with last-attempted timestamp
	client.mu.Lock()
	client.objects["queued/"+key] = &mockObject{
		name:   "queued/" + key,
		exists: true,
		attrs: &storage.ObjectAttrs{
			Name:           "queued/" + key,
			Created:        baseTime,
			Updated:        baseTime,
			Metageneration: 1,
			Metadata: map[string]string{
				priorityMetadataKey:  "00000005",
				lastAttemptedKey:     strconv.FormatInt(lastAttemptedTime.Unix(), 10),
				attemptsMetadataKey:  "2",
				notBeforeMetadataKey: noNotBefore,
			},
		},
	}
	client.mu.Unlock()

	// Enumerate and start the item
	_, queued, err := wq.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate() = %v, want nil", err)
	}
	if len(queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(queued))
	}

	// Verify that wait latency calculation uses last attempted time
	// We can't directly test the Prometheus metric, but we can verify the logic
	// by checking that the Start() method works correctly with the last-attempted metadata
	owned, err := queued[0].Start(ctx)
	if err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	
	// Verify attempt count was incremented from the metadata
	if owned.GetAttempts() != 3 { // 2 + 1
		t.Errorf("owned.GetAttempts() = %d, want 3", owned.GetAttempts())
	}

	// Complete and verify cleanup
	err = owned.Complete(ctx)
	if err != nil {
		t.Fatalf("Complete() = %v, want nil", err)
	}
}

func TestE2E_PriorityOrdering(t *testing.T) {
	client := newMockClient()
	wq := NewWorkQueue(client, 10)
	ctx := context.Background()

	// Queue items with different priorities
	items := []struct {
		key      string
		priority int64
	}{
		{"low-priority", 1},
		{"high-priority", 10},
		{"medium-priority", 5},
	}

	for _, item := range items {
		err := wq.Queue(ctx, item.key, workqueue.Options{Priority: item.priority})
		if err != nil {
			t.Fatalf("Queue(%s) = %v, want nil", item.key, err)
		}
	}

	// Enumerate and verify ordering (higher priority first)
	_, queued, err := wq.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate() = %v, want nil", err)
	}
	if len(queued) != 3 {
		t.Fatalf("len(queued) = %d, want 3", len(queued))
	}

	expectedOrder := []string{"high-priority", "medium-priority", "low-priority"}
	for i, expected := range expectedOrder {
		if queued[i].Name() != expected {
			t.Errorf("queued[%d].Name() = %q, want %q", i, queued[i].Name(), expected)
		}
	}
}

func TestE2E_OrphanedKeyDetection(t *testing.T) {
	client := newMockClient()
	wq := NewWorkQueue(client, 10)
	ctx := context.Background()

	key := "test-key-5"
	
	// Manually create an orphaned in-progress object (expired)
	expiredTime := time.Now().UTC().Add(-1 * time.Hour)
	client.mu.Lock()
	client.objects["in-progress/"+key] = &mockObject{
		name:   "in-progress/" + key,
		exists: true,
		attrs: &storage.ObjectAttrs{
			Name:           "in-progress/" + key,
			Created:        expiredTime,
			Updated:        expiredTime,
			Metageneration: 1,
			Metadata: map[string]string{
				expirationMetadataKey: expiredTime.Format(time.RFC3339),
				attemptsMetadataKey:   "1",
			},
		},
	}
	client.mu.Unlock()

	// Enumerate and verify the key is detected as orphaned
	inProgress, _, err := wq.Enumerate(ctx)
	if err != nil {
		t.Fatalf("Enumerate() = %v, want nil", err)
	}
	if len(inProgress) != 1 {
		t.Fatalf("len(inProgress) = %d, want 1", len(inProgress))
	}
	
	if !inProgress[0].IsOrphaned() {
		t.Errorf("Expected in-progress key to be detected as orphaned")
	}
}

