//go:build integration

// Package integration_test exercises the adapters against the local Compose services.
package integration_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"async-file-processor/internal/config"
	"async-file-processor/internal/jobs"
	"async-file-processor/internal/queue"
	"async-file-processor/internal/storage"
	"async-file-processor/migrations"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func loadConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestStorage(t *testing.T) {
	cfg := loadConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := storage.New(ctx, storage.Config{Region: cfg.AWSRegion, Endpoint: cfg.S3Endpoint, Bucket: cfg.S3Bucket})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureBucket(ctx); err != nil {
		t.Fatalf("object storage unavailable (run make up): %v", err)
	}
	if err := store.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket is not idempotent: %v", err)
	}
	key := "integration-test/" + uuid.NewString() + ".csv"
	t.Cleanup(func() { _ = store.Delete(context.Background(), key) })

	// Larger than the uploader part size so the multipart path is exercised.
	body := strings.Repeat("id,name\n", 2<<20)
	if err := store.Put(ctx, key, strings.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	rc, err := store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Fatalf("downloaded %d bytes, want %d", len(got), len(body))
	}
	if _, err := store.Get(ctx, "integration-test/missing-"+uuid.NewString()); err == nil {
		t.Fatal("expected error for missing object")
	}
}

func TestQueue(t *testing.T) {
	cfg := loadConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	q, err := queue.New(ctx, queue.Config{Region: cfg.AWSRegion, Endpoint: cfg.SQSEndpoint, QueueURL: cfg.SQSQueueURL})
	if err != nil {
		t.Fatal(err)
	}
	body := "integration-" + uuid.NewString()
	if err := q.Send(ctx, body); err != nil {
		t.Fatalf("queue unavailable (run make up): %v", err)
	}
	var msg *queue.Msg
	for deadline := time.Now().Add(15 * time.Second); msg == nil && time.Now().Before(deadline); {
		msgs, err := q.Receive(ctx, 10, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		for i := range msgs {
			// Release messages that belong to real jobs instead of deleting them.
			if msgs[i].Body == body {
				msg = &msgs[i]
			} else if err := q.ChangeVisibility(ctx, msgs[i].Receipt, 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	if msg == nil {
		t.Fatal("sent message was not received")
	}
	if msg.ReceiveCount != 1 {
		t.Fatalf("receive count = %d, want 1", msg.ReceiveCount)
	}
	if err := q.ChangeVisibility(ctx, msg.Receipt, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := q.Delete(ctx, msg.Receipt); err != nil {
		t.Fatal(err)
	}
	msgs, err := q.Receive(ctx, 10, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range msgs {
		if m.Body == body {
			t.Fatal("deleted message was received again")
		}
	}
}

func TestChecks(t *testing.T) {
	cfg := loadConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := storage.New(ctx, storage.Config{Region: cfg.AWSRegion, Endpoint: cfg.S3Endpoint, Bucket: cfg.S3Bucket})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureBucket(ctx); err != nil {
		t.Fatalf("object storage unavailable (run make up): %v", err)
	}
	if err := store.Check(ctx); err != nil {
		t.Fatalf("storage check: %v", err)
	}
	q, err := queue.New(ctx, queue.Config{Region: cfg.AWSRegion, Endpoint: cfg.SQSEndpoint, QueueURL: cfg.SQSQueueURL})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Check(ctx); err != nil {
		t.Fatalf("queue check (run make up): %v", err)
	}
	if n, err := q.MaxReceiveCount(ctx); err != nil || n != cfg.MaxAttempts {
		t.Fatalf("MaxReceiveCount = %d, %v; want %d", n, err, cfg.MaxAttempts)
	}
	missing, err := queue.New(ctx, queue.Config{Region: cfg.AWSRegion, Endpoint: cfg.SQSEndpoint, QueueURL: cfg.SQSEndpoint + "/000000000000/no-such-queue"})
	if err != nil {
		t.Fatal(err)
	}
	if err := missing.Check(ctx); err == nil {
		t.Fatal("expected an error for a missing queue")
	}
}

func TestJobs(t *testing.T) {
	cfg := loadConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("database unavailable (run make up): %v", err)
	}
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("migrations are not idempotent: %v", err)
	}
	repo := jobs.NewRepo(pool)
	owner := []byte(uuid.NewString()) // a unique owner per test run
	clientKey := "client-" + uuid.NewString()
	newJob := func(t *testing.T) string {
		t.Helper()
		id := uuid.NewString()
		t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", id) })
		j, err := repo.Create(ctx, jobs.NewJob{ID: id, Filename: "data.csv", InputKey: "uploads/" + id + ".csv", OwnerHash: owner, ClientKey: clientKey})
		if err != nil {
			t.Fatal(err)
		}
		if j.Status != jobs.StatusPending || j.Filename != "data.csv" || j.Attempts != 0 {
			t.Fatalf("unexpected new job: %+v", j)
		}
		return id
	}

	t.Run("expired jobs", func(t *testing.T) {
		done, failed, active := newJob(t), newJob(t), newJob(t)
		if err := repo.Complete(ctx, done, "", "", 1, 1, 0); err != nil {
			t.Fatal(err)
		}
		if err := repo.Fail(ctx, failed, "boom"); err != nil {
			t.Fatal(err)
		}
		contains := func(list []jobs.Job, id string) bool {
			for _, j := range list {
				if j.ID == id {
					return true
				}
			}
			return false
		}
		list, err := repo.ListExpired(ctx, time.Now().Add(-time.Hour), 1000)
		if err != nil || contains(list, done) || contains(list, failed) {
			t.Fatalf("recent jobs listed as expired: %v", err)
		}
		list, err = repo.ListExpired(ctx, time.Now().Add(time.Minute), 1000)
		if err != nil || !contains(list, done) || !contains(list, failed) {
			t.Fatalf("finished jobs missing from expired list: %v", err)
		}
		if contains(list, active) {
			t.Fatal("a PENDING job must never be expired")
		}
		if ok, err := repo.DeleteFinished(ctx, active); err != nil || ok {
			t.Fatalf("DeleteFinished(active) = %v, %v; want false, nil", ok, err)
		}
		if ok, err := repo.DeleteFinished(ctx, done); err != nil || !ok {
			t.Fatalf("DeleteFinished(done) = %v, %v", ok, err)
		}
		if ok, err := repo.DeleteFinished(ctx, done); err != nil || ok {
			t.Fatalf("second DeleteFinished = %v, %v; want false, nil", ok, err)
		}
		if _, err := repo.Get(ctx, done); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("deleted job still present: %v", err)
		}
	})

	t.Run("get and list", func(t *testing.T) {
		id := newJob(t)
		got, err := repo.Get(ctx, id)
		if err != nil || got.ID != id {
			t.Fatalf("Get = %+v, %v", got, err)
		}
		if _, err := repo.Get(ctx, uuid.NewString()); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("Get unknown id: %v, want ErrNotFound", err)
		}
		list, err := repo.List(ctx, owner, 1)
		if err != nil || len(list) != 1 {
			t.Fatalf("List = %d jobs, %v", len(list), err)
		}
		newest := newJob(t)
		list, err = repo.List(ctx, owner, 5)
		if err != nil || len(list) == 0 || list[0].ID != newest {
			t.Fatalf("List is not newest first: %v", err)
		}
		for _, j := range list {
			if j.Filename != "data.csv" {
				t.Fatalf("unexpected job in list: %+v", j)
			}
		}
	})

	t.Run("list is scoped to the owner", func(t *testing.T) {
		mine := newJob(t)
		other := uuid.NewString()
		t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", other) })
		if _, err := repo.Create(ctx, jobs.NewJob{ID: other, Filename: "o.csv", InputKey: other + "/input.csv", OwnerHash: []byte("someone-else")}); err != nil {
			t.Fatal(err)
		}
		list, err := repo.List(ctx, owner, 200)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, j := range list {
			if j.ID == other {
				t.Fatal("List returned another owner's job")
			}
			found = found || j.ID == mine
		}
		if !found {
			t.Fatal("List did not return the owner's job")
		}
		if none, err := repo.List(ctx, nil, 200); err != nil || len(none) != 0 {
			t.Fatalf("List without an owner = %d jobs, %v; want none", len(none), err)
		}
	})

	t.Run("unfinished counts", func(t *testing.T) {
		_, before, err := repo.Unfinished(ctx, clientKey)
		if err != nil {
			t.Fatal(err)
		}
		a, b := newJob(t), newJob(t)
		total, mine, err := repo.Unfinished(ctx, clientKey)
		if err != nil || mine != before+2 || total < mine {
			t.Fatalf("Unfinished = %d, %d, %v; want client count %d", total, mine, err, before+2)
		}
		if err := repo.Complete(ctx, a, "", "", 0, 0, 0); err != nil {
			t.Fatal(err)
		}
		if err := repo.Fail(ctx, b, "x"); err != nil {
			t.Fatal(err)
		}
		if _, mine, err = repo.Unfinished(ctx, clientKey); err != nil || mine != before {
			t.Fatalf("finished jobs still counted: %d, %v; want %d", mine, err, before)
		}
	})

	t.Run("claim and complete", func(t *testing.T) {
		id := newJob(t)
		j, err := repo.MarkProcessing(ctx, id)
		if err != nil || j.Status != jobs.StatusProcessing || j.StartedAt == nil || j.Attempts != 1 {
			t.Fatalf("MarkProcessing = %+v, %v", j, err)
		}
		if err := repo.UpdateProgress(ctx, id, 10, 8, 2); err != nil {
			t.Fatal(err)
		}
		j, err = repo.MarkProcessing(ctx, id)
		if err != nil {
			t.Fatalf("reclaiming a PROCESSING job: %v", err)
		}
		if j.Attempts != 2 || j.TotalRows != 0 || j.ValidRows != 0 || j.InvalidRows != 0 ||
			j.ErrorMsg != nil || j.ReportKey != nil || j.ErrorsKey != nil {
			t.Fatalf("reclaim must count an attempt and reset counters: %+v", j)
		}
		if err := repo.Complete(ctx, id, "reports/"+id+".json", "", 100, 90, 10); err != nil {
			t.Fatal(err)
		}
		got, err := repo.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != jobs.StatusCompleted || got.TotalRows != 100 || got.ValidRows != 90 || got.InvalidRows != 10 ||
			got.ReportKey == nil || got.ErrorsKey != nil || got.FinishedAt == nil || got.Attempts != 2 {
			t.Fatalf("unexpected completed job: %+v", got)
		}
		if _, err := repo.MarkProcessing(ctx, id); !errors.Is(err, jobs.ErrNotClaimable) {
			t.Fatalf("MarkProcessing on COMPLETED: %v, want ErrNotClaimable", err)
		}
	})

	t.Run("fail if active", func(t *testing.T) {
		stuck := newJob(t)
		if _, err := repo.MarkProcessing(ctx, stuck); err != nil {
			t.Fatal(err)
		}
		changed, err := repo.FailIfActive(ctx, stuck, "moved to dead-letter queue after 3 receives")
		if err != nil || !changed {
			t.Fatalf("FailIfActive on PROCESSING = %v, %v", changed, err)
		}
		got, err := repo.Get(ctx, stuck)
		if err != nil || got.Status != jobs.StatusFailed || got.FinishedAt == nil ||
			got.ErrorMsg == nil || *got.ErrorMsg != "moved to dead-letter queue after 3 receives" {
			t.Fatalf("unexpected job after FailIfActive: %+v, %v", got, err)
		}
		// Finished jobs and unknown ids are left alone.
		if changed, err := repo.FailIfActive(ctx, stuck, "again"); err != nil || changed {
			t.Fatalf("FailIfActive on FAILED = %v, %v", changed, err)
		}
		done := newJob(t)
		if err := repo.Complete(ctx, done, "", "", 1, 1, 0); err != nil {
			t.Fatal(err)
		}
		if changed, err := repo.FailIfActive(ctx, done, "x"); err != nil || changed {
			t.Fatalf("FailIfActive on COMPLETED = %v, %v", changed, err)
		}
		if got, _ := repo.Get(ctx, done); got.Status != jobs.StatusCompleted {
			t.Fatalf("COMPLETED job was modified: %+v", got)
		}
		if changed, err := repo.FailIfActive(ctx, uuid.NewString(), "x"); err != nil || changed {
			t.Fatalf("FailIfActive on unknown id = %v, %v", changed, err)
		}
	})

	t.Run("fail", func(t *testing.T) {
		id := newJob(t)
		if err := repo.Fail(ctx, id, "bad header"); err != nil {
			t.Fatal(err)
		}
		got, err := repo.Get(ctx, id)
		if err != nil || got.Status != jobs.StatusFailed || got.ErrorMsg == nil || *got.ErrorMsg != "bad header" {
			t.Fatalf("unexpected failed job: %+v, %v", got, err)
		}
		if _, err := repo.MarkProcessing(ctx, id); !errors.Is(err, jobs.ErrNotClaimable) {
			t.Fatalf("MarkProcessing on FAILED: %v, want ErrNotClaimable", err)
		}
		if _, err := repo.MarkProcessing(ctx, uuid.NewString()); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("MarkProcessing unknown id: %v, want ErrNotFound", err)
		}
	})
}
