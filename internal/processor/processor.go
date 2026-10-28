// Package processor streams an uploaded CSV through validation and writes the invalid rows to
// an errors file, without holding the input or the output in memory.
package processor

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"

	"async-file-processor/internal/jobs"
	"async-file-processor/internal/validate"
)

// ProgressEvery is the number of rows between progress updates.
const ProgressEvery = 5000

// ErrInvalidInput marks input that can never be processed successfully, such as a missing or
// wrong header. Retrying the job will not help.
var ErrInvalidInput = errors.New("invalid input file")

// errorsHeader is the first line of every errors file.
var errorsHeader = []string{"row", "field", "error", "raw"}

// Store is the subset of object storage the processor uses.
type Store interface {
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Put(ctx context.Context, key string, r io.Reader) error
}

// Progress receives running row counts while a job is processed.
type Progress interface {
	UpdateProgress(ctx context.Context, id string, total, valid, invalid int64) error
}

// Result is the outcome of processing one job.
type Result struct {
	ReportKey string
	ErrorsKey string
	Total     int64
	Valid     int64
	Invalid   int64
}

// Processor validates CSV files stored in object storage.
type Processor struct {
	store    Store
	progress Progress
	logger   *slog.Logger
}

// New returns a Processor.
func New(store Store, progress Progress, logger *slog.Logger) *Processor {
	return &Processor{store: store, progress: progress, logger: logger}
}

// ErrorsKey returns the object key of the errors file for a job.
func ErrorsKey(jobID string) string { return jobID + "/errors.csv" }

// Process reads the job's input, validates every row, and uploads {id}/errors.csv with one line
// per failed rule. The errors file is always written, header only when every row is valid.
//
// Input that cannot be processed (empty file, wrong header, unreadable CSV structure) returns an
// error wrapping ErrInvalidInput. A row that is malformed CSV (for example a stray quote) is
// reported as a row error and processing continues with the next line.
func (p *Processor) Process(ctx context.Context, job jobs.Job) (res Result, err error) {
	body, err := p.store.Get(ctx, job.InputKey)
	if err != nil {
		return Result{}, fmt.Errorf("read input: %w", err)
	}
	defer body.Close()

	r := csv.NewReader(body)
	r.ReuseRecord = true
	r.FieldsPerRecord = -1 // width is checked per row so a bad row does not abort the file

	header, err := r.Read()
	switch {
	case errors.Is(err, io.EOF):
		return Result{}, fmt.Errorf("%w: file is empty, expected header %s", ErrInvalidInput, strings.Join(validate.Header, ","))
	case err != nil:
		return Result{}, fmt.Errorf("%w: read header: %v", ErrInvalidInput, err)
	}
	if err := validate.ValidateHeader(header); err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	// The errors file is uploaded while it is being produced.
	key := ErrorsKey(job.ID)
	pr, pw := io.Pipe()
	uploaded := make(chan error, 1)
	go func() {
		err := p.store.Put(ctx, key, pr)
		pr.CloseWithError(err) // unblocks the writer if the upload stops early; nil acts like io.ErrClosedPipe
		uploaded <- err
	}()
	defer func() {
		// Close the pipe with the processing error so a partial upload is aborted, then wait for
		// the uploader. An upload failure surfaces only when processing itself succeeded.
		pw.CloseWithError(err)
		if uploadErr := <-uploaded; err == nil && uploadErr != nil {
			res, err = Result{}, fmt.Errorf("upload errors file: %w", uploadErr)
		}
	}()

	out := csv.NewWriter(pw)
	if err := out.Write(errorsHeader); err != nil {
		return Result{}, fmt.Errorf("write errors file: %w", err)
	}

	v := validate.NewValidator()
	var rawBuf bytes.Buffer
	raw := csv.NewWriter(&rawBuf)
	rawOf := func(rec []string) string {
		rawBuf.Reset()
		raw.Write(rec)
		raw.Flush()
		return strings.TrimRight(rawBuf.String(), "\r\n")
	}

	for {
		rec, readErr := r.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		var perr *csv.ParseError
		var errs []validate.FieldError
		var rawRec string
		switch {
		case errors.As(readErr, &perr):
			errs = []validate.FieldError{{Field: validate.RowField, Message: fmt.Sprintf("malformed CSV at line %d, column %d: %v", perr.Line, perr.Column, perr.Err)}}
		case readErr != nil:
			return Result{}, fmt.Errorf("read input: %w", readErr)
		default:
			errs = v.ValidateRow(rec)
			if len(errs) > 0 {
				rawRec = rawOf(rec)
			}
		}

		res.Total++
		if len(errs) == 0 {
			res.Valid++
		} else {
			res.Invalid++
			rowNum := strconv.FormatInt(res.Total, 10)
			for _, fe := range errs {
				if err := out.Write([]string{rowNum, fe.Field, fe.Message, rawRec}); err != nil {
					return Result{}, fmt.Errorf("write errors file: %w", err)
				}
			}
		}
		if res.Total%ProgressEvery == 0 {
			if err := p.progress.UpdateProgress(ctx, job.ID, res.Total, res.Valid, res.Invalid); err != nil {
				p.logger.Warn("update progress failed", "job_id", job.ID, "error", err)
			}
		}
	}

	out.Flush()
	if err := out.Error(); err != nil {
		return Result{}, fmt.Errorf("write errors file: %w", err)
	}
	res.ErrorsKey = key
	return res, nil
}
