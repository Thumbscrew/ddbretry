package ddbretry

import (
	"context"
	"errors"
	"time"

	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamoDBClient interface {
	GetItem(context.Context, *ddb.GetItemInput, ...func(*ddb.Options)) (*ddb.GetItemOutput, error)
	DeleteItem(context.Context, *ddb.DeleteItemInput, ...func(*ddb.Options)) (*ddb.DeleteItemOutput, error)
	PutItem(context.Context, *ddb.PutItemInput, ...func(*ddb.Options)) (*ddb.PutItemOutput, error)
}

type RetryDynamoDBClient struct {
	DynamoDBClient
	Retries     int
	BackOffTime time.Duration
}

func NewRetryDynamoDBClient(client DynamoDBClient, retries int, backOff time.Duration) *RetryDynamoDBClient {
	return &RetryDynamoDBClient{
		DynamoDBClient: client,
		Retries:        retries,
		BackOffTime:    backOff,
	}
}

func (c *RetryDynamoDBClient) GetItem(ctx context.Context, input *ddb.GetItemInput, o ...func(*ddb.Options)) (output *ddb.GetItemOutput, err error) {
	retries := c.Retries
	infinite := retries == -1
	for retries >= 0 || infinite {
		output, err = c.DynamoDBClient.GetItem(ctx, input, o...)
		if err != nil {
			if IsProvisionedThroughputExceededException(err) {
				if retries > 0 {
					retries--
					time.Sleep(c.BackOffTime)
				} else if infinite {
					time.Sleep(c.BackOffTime)
				} else {
					return
				}
			} else {
				return
			}
		} else {
			return
		}
	}

	return nil, NewInvalidRetryError(retries)
}

func (c *RetryDynamoDBClient) DeleteItem(ctx context.Context, input *ddb.DeleteItemInput, o ...func(*ddb.Options)) (output *ddb.DeleteItemOutput, err error) {
	retries := c.Retries
	infinite := retries == -1
	for retries >= 0 || infinite {
		output, err = c.DynamoDBClient.DeleteItem(ctx, input, o...)
		if err != nil {
			if IsProvisionedThroughputExceededException(err) {
				if retries > 0 {
					retries--
					time.Sleep(c.BackOffTime)
				} else if infinite {
					time.Sleep(c.BackOffTime)
				} else {
					return
				}
			} else {
				return
			}
		} else {
			return
		}
	}

	return nil, NewInvalidRetryError(retries)
}

func (c *RetryDynamoDBClient) PutItem(ctx context.Context, input *ddb.PutItemInput, o ...func(*ddb.Options)) (output *ddb.PutItemOutput, err error) {
	retries := c.Retries
	infinite := retries == -1
	for retries >= 0 || infinite {
		output, err = c.DynamoDBClient.PutItem(ctx, input, o...)
		if err != nil {
			if IsProvisionedThroughputExceededException(err) {
				if retries > 0 {
					retries--
					time.Sleep(c.BackOffTime)
				} else if infinite {
					time.Sleep(c.BackOffTime)
				} else {
					return
				}
			} else {
				return
			}
		} else {
			return
		}
	}

	return nil, NewInvalidRetryError(retries)
}

func IsProvisionedThroughputExceededException(err error) bool {
	var provisionedThroughputExceededException *types.ProvisionedThroughputExceededException
	ok := errors.As(err, &provisionedThroughputExceededException)

	return ok
}
