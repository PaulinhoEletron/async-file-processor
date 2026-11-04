// Package queue provides SQS messaging operations.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// MaxWait is the longest long-poll duration SQS allows.
const MaxWait = 20 * time.Second

// Config selects the queue and, for local development, an SQS-compatible endpoint.
type Config struct {
	Region   string
	Endpoint string // empty uses the AWS default
	QueueURL string
}

// Msg is a received message. Receipt identifies it for Delete and ChangeVisibility.
type Msg struct {
	ID           string
	Body         string
	Receipt      string
	ReceiveCount int
}

// Queue sends and receives messages on a single SQS queue.
type Queue struct {
	client *sqs.Client
	url    string
}

// New builds a Queue. Credentials come from the SDK default chain.
func New(ctx context.Context, cfg Config) (*Queue, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	return &Queue{client: client, url: cfg.QueueURL}, nil
}

// Send enqueues body.
func (q *Queue) Send(ctx context.Context, body string) error {
	if _, err := q.client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: &q.url, MessageBody: &body}); err != nil {
		return fmt.Errorf("send message: %w", err)
	}
	return nil
}

// Receive long-polls for up to limit messages (clamped to 1..10), waiting at most wait (capped at MaxWait).
func (q *Queue) Receive(ctx context.Context, limit int, wait time.Duration) ([]Msg, error) {
	limit = min(max(limit, 1), 10)
	wait = min(wait, MaxWait)
	out, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &q.url,
		MaxNumberOfMessages: int32(limit),
		WaitTimeSeconds:     int32(wait / time.Second),
		MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{
			sqstypes.MessageSystemAttributeNameApproximateReceiveCount,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("receive messages: %w", err)
	}
	msgs := make([]Msg, 0, len(out.Messages))
	for _, m := range out.Messages {
		msg := Msg{ID: aws.ToString(m.MessageId), Body: aws.ToString(m.Body), Receipt: aws.ToString(m.ReceiptHandle)}
		fmt.Sscan(m.Attributes[string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount)], &msg.ReceiveCount)
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// Delete removes a processed message.
func (q *Queue) Delete(ctx context.Context, receipt string) error {
	if _, err := q.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &q.url, ReceiptHandle: &receipt}); err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	return nil
}

// ChangeVisibility sets how long a received message stays hidden from other consumers.
func (q *Queue) ChangeVisibility(ctx context.Context, receipt string, timeout time.Duration) error {
	_, err := q.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &q.url,
		ReceiptHandle:     &receipt,
		VisibilityTimeout: int32(timeout / time.Second),
	})
	if err != nil {
		return fmt.Errorf("change message visibility: %w", err)
	}
	return nil
}

// Check verifies that the queue is reachable.
func (q *Queue) Check(ctx context.Context) error {
	_, err := q.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &q.url,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return fmt.Errorf("get queue attributes: %w", err)
	}
	return nil
}

// MaxReceiveCount returns the maxReceiveCount of the queue's redrive policy, or 0 when the
// queue has no dead-letter queue.
func (q *Queue) MaxReceiveCount(ctx context.Context) (int, error) {
	out, err := q.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &q.url,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameRedrivePolicy},
	})
	if err != nil {
		return 0, fmt.Errorf("get queue attributes: %w", err)
	}
	raw := out.Attributes[string(sqstypes.QueueAttributeNameRedrivePolicy)]
	if raw == "" {
		return 0, nil
	}
	var policy struct {
		MaxReceiveCount json.Number `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return 0, fmt.Errorf("parse redrive policy: %w", err)
	}
	n, err := policy.MaxReceiveCount.Int64()
	if err != nil {
		return 0, fmt.Errorf("parse maxReceiveCount: %w", err)
	}
	return int(n), nil
}
