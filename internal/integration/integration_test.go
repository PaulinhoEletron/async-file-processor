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
	newJob := func(t *testing.T) string {
		t.Helper()
		id := uuid.NewString()
		t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", id) })
		j, err := repo.Create(ctx, id, "data.csv", "uploads/"+id+".csv")
		if err != nil {
			t.Fatal(err)
		}
		if j.Status != jobs.StatusPending || j.Filename != "data.csv" || j.Attempts != 0 {
			t.Fatalf("unexpected new job: %+v", j)
		}
		return id
	}

	t.Run("get and list", func(t *testing.T) {
		id := newJob(t)
		got, err := repo.Get(ctx, id)
		if err != nil || got.ID != id {
			t.Fatalf("Get = %+v, %v", got, err)
		}
		if _, err := repo.Get(ctx, uuid.NewString()); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("Get unknown id: %v, want ErrNotFound", err)
		}
		list, err := repo.List(ctx, 1)
		if err != nil || len(list) != 1 {
			t.Fatalf("List = %d jobs, %v", len(list), err)
		}
		newest := newJob(t)
		list, err = repo.List(ctx, 5)
		if err != nil || len(list) == 0 || list[0].ID != newest {
			t.Fatalf("List is not newest first: %v", err)
		}
	})

	t.Run("claim and complete", func(t *testing.T) {
		id := newJob(t)
		j, err := repo.MarkProcessing(ctx, id)
		if err != nil || j.Status != jobs.StatusProcessing || j.StartedAt == nil {
			t.Fatalf("MarkProcessing = %+v, %v", j, err)
		}
		if _, err := repo.MarkProcessing(ctx, id); err != nil {
			t.Fatalf("reclaiming a PROCESSING job: %v", err)
		}
		if err := repo.UpdateProgress(ctx, id, 10, 8, 2); err != nil {
			t.Fatal(err)
		}
		if n, err := repo.IncAttempts(ctx, id); err != nil || n != 1 {
			t.Fatalf("IncAttempts = %d, %v", n, err)
		}
		if err := repo.Complete(ctx, id, "reports/"+id+".json", "", 100, 90, 10); err != nil {
			t.Fatal(err)
		}
		got, err := repo.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != jobs.StatusCompleted || got.TotalRows != 100 || got.ValidRows != 90 || got.InvalidRows != 10 ||
			got.ReportKey == nil || got.ErrorsKey != nil || got.FinishedAt == nil || got.Attempts != 1 {
			t.Fatalf("unexpected completed job: %+v", got)
		}
		if _, err := repo.MarkProcessing(ctx, id); !errors.Is(err, jobs.ErrNotClaimable) {
			t.Fatalf("MarkProcessing on COMPLETED: %v, want ErrNotClaimable", err)
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
