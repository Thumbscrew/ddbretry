package ddbretry

import (
	"context"
	"errors"
	"testing"
	"time"

	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
)

func TestIsProvisionedThroughputExceededException(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "should return true when error is ProvisionedThroughputExceededException",
			args: args{
				err: &types.ProvisionedThroughputExceededException{},
			},
			want: true,
		},
		{
			name: "should return false when error is not ProvisionedThroughputExceededException",
			args: args{
				err: errors.New("foo"),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsProvisionedThroughputExceededException(tt.args.err))
		})
	}
}

type SuccessfulDynamoDBClient struct {
	ThroughputExceededCount int
}

func (c *SuccessfulDynamoDBClient) GetItem(ctx context.Context, input *ddb.GetItemInput, o ...func(*ddb.Options)) (*ddb.GetItemOutput, error) {
	for c.ThroughputExceededCount > 0 {
		c.ThroughputExceededCount--
		return nil, &types.ProvisionedThroughputExceededException{}
	}

	return &ddb.GetItemOutput{}, nil
}

func (c *SuccessfulDynamoDBClient) DeleteItem(ctx context.Context, input *ddb.DeleteItemInput, o ...func(*ddb.Options)) (*ddb.DeleteItemOutput, error) {
	for c.ThroughputExceededCount > 0 {
		c.ThroughputExceededCount--
		return nil, &types.ProvisionedThroughputExceededException{}
	}

	return &ddb.DeleteItemOutput{}, nil
}

func (c *SuccessfulDynamoDBClient) PutItem(ctx context.Context, input *ddb.PutItemInput, o ...func(*ddb.Options)) (*ddb.PutItemOutput, error) {
	for c.ThroughputExceededCount > 0 {
		c.ThroughputExceededCount--
		return nil, &types.ProvisionedThroughputExceededException{}
	}

	return &ddb.PutItemOutput{}, nil
}

type FailingDynamoDBClient struct {
	ThroughputExceededCount int
	Err                     error
}

func (c *FailingDynamoDBClient) GetItem(ctx context.Context, input *ddb.GetItemInput, o ...func(*ddb.Options)) (*ddb.GetItemOutput, error) {
	for c.ThroughputExceededCount > 0 {
		c.ThroughputExceededCount--
		return nil, &types.ProvisionedThroughputExceededException{}
	}

	return nil, c.Err
}

func (c *FailingDynamoDBClient) DeleteItem(ctx context.Context, input *ddb.DeleteItemInput, o ...func(*ddb.Options)) (*ddb.DeleteItemOutput, error) {
	for c.ThroughputExceededCount > 0 {
		c.ThroughputExceededCount--
		return nil, &types.ProvisionedThroughputExceededException{}
	}

	return nil, c.Err
}

func (c *FailingDynamoDBClient) PutItem(ctx context.Context, input *ddb.PutItemInput, o ...func(*ddb.Options)) (*ddb.PutItemOutput, error) {
	for c.ThroughputExceededCount > 0 {
		c.ThroughputExceededCount--
		return nil, &types.ProvisionedThroughputExceededException{}
	}

	return nil, c.Err
}

func TestRetryDynamoDBClient(t *testing.T) {
	ctx := context.Background()

	type fields struct {
		GetItemDynamoDBClient    DynamoDBClient
		DeleteItemDynamoDBClient DynamoDBClient
		PutItemDynamoDBClient    DynamoDBClient
		Retries                  int
		BackOffTime              time.Duration
	}
	type args struct {
		ctx             context.Context
		getItemInput    *ddb.GetItemInput
		deleteItemInput *ddb.DeleteItemInput
		putItemInput    *ddb.PutItemInput
		o               []func(*ddb.Options)
	}
	tests := []struct {
		name                 string
		fields               fields
		args                 args
		wantGetItemOutput    *ddb.GetItemOutput
		wantDeleteItemOutput *ddb.DeleteItemOutput
		wantPutItemOutput    *ddb.PutItemOutput
		wantErr              error
	}{
		{
			name: "should receive output from successful call in DynamoDBClient",
			fields: fields{
				GetItemDynamoDBClient:    &SuccessfulDynamoDBClient{},
				DeleteItemDynamoDBClient: &SuccessfulDynamoDBClient{},
				PutItemDynamoDBClient:    &SuccessfulDynamoDBClient{},
			},
			args: args{
				ctx:             ctx,
				getItemInput:    &ddb.GetItemInput{},
				deleteItemInput: &ddb.DeleteItemInput{},
				putItemInput:    &ddb.PutItemInput{},
			},
			wantGetItemOutput:    &ddb.GetItemOutput{},
			wantDeleteItemOutput: &ddb.DeleteItemOutput{},
			wantPutItemOutput:    &ddb.PutItemOutput{},
			wantErr:              nil,
		},
		{
			name: "should receive error from failed call in DynamoDBClient",
			fields: fields{
				GetItemDynamoDBClient: &FailingDynamoDBClient{
					Err: errors.New("foo"),
				},
				DeleteItemDynamoDBClient: &FailingDynamoDBClient{
					Err: errors.New("foo"),
				},
				PutItemDynamoDBClient: &FailingDynamoDBClient{
					Err: errors.New("foo"),
				},
			},
			args: args{
				ctx:             ctx,
				getItemInput:    &ddb.GetItemInput{},
				deleteItemInput: &ddb.DeleteItemInput{},
				putItemInput:    &ddb.PutItemInput{},
			},
			wantGetItemOutput:    nil,
			wantDeleteItemOutput: nil,
			wantPutItemOutput:    nil,
			wantErr:              errors.New("foo"),
		},
		{
			name: "should receive output when retries is higher than number of throughput exceptions",
			fields: fields{
				GetItemDynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 2,
				},
				DeleteItemDynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 2,
				},
				PutItemDynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 2,
				},
				Retries: 3,
			},
			args: args{
				ctx:             ctx,
				getItemInput:    &ddb.GetItemInput{},
				deleteItemInput: &ddb.DeleteItemInput{},
				putItemInput:    &ddb.PutItemInput{},
			},
			wantGetItemOutput:    &ddb.GetItemOutput{},
			wantDeleteItemOutput: &ddb.DeleteItemOutput{},
			wantPutItemOutput:    &ddb.PutItemOutput{},
			wantErr:              nil,
		},
		{
			name: "should receive throughput exception when number of throughput exceptions is higher than retries",
			fields: fields{
				GetItemDynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 3,
				},
				DeleteItemDynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 3,
				},
				PutItemDynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 3,
				},
				Retries: 2,
			},
			args: args{
				ctx:             ctx,
				getItemInput:    &ddb.GetItemInput{},
				deleteItemInput: &ddb.DeleteItemInput{},
				putItemInput:    &ddb.PutItemInput{},
			},
			wantGetItemOutput:    nil,
			wantDeleteItemOutput: nil,
			wantPutItemOutput:    nil,
			wantErr:              &types.ProvisionedThroughputExceededException{},
		},
		{
			name: "should receive error after throughput exceptions when retries is higher",
			fields: fields{
				GetItemDynamoDBClient: &FailingDynamoDBClient{
					ThroughputExceededCount: 2,
					Err:                     errors.New("foo"),
				},
				DeleteItemDynamoDBClient: &FailingDynamoDBClient{
					ThroughputExceededCount: 2,
					Err:                     errors.New("foo"),
				},
				PutItemDynamoDBClient: &FailingDynamoDBClient{
					ThroughputExceededCount: 2,
					Err:                     errors.New("foo"),
				},
				Retries: 3,
			},
			args: args{
				ctx:             ctx,
				getItemInput:    &ddb.GetItemInput{},
				deleteItemInput: &ddb.DeleteItemInput{},
				putItemInput:    &ddb.PutItemInput{},
			},
			wantGetItemOutput:    nil,
			wantDeleteItemOutput: nil,
			wantPutItemOutput:    nil,
			wantErr:              errors.New("foo"),
		},
		{
			name: "should receive output after throughput exceptions when retries is infinite",
			fields: fields{
				GetItemDynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 10,
				},
				DeleteItemDynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 10,
				},
				PutItemDynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 10,
				},
				Retries: -1,
			},
			args: args{
				ctx:             ctx,
				getItemInput:    &ddb.GetItemInput{},
				deleteItemInput: &ddb.DeleteItemInput{},
				putItemInput:    &ddb.PutItemInput{},
			},
			wantGetItemOutput:    &ddb.GetItemOutput{},
			wantDeleteItemOutput: &ddb.DeleteItemOutput{},
			wantPutItemOutput:    &ddb.PutItemOutput{},
			wantErr:              nil,
		},
		{
			name: "should receive InvalidRetryError when retries value is invalid",
			fields: fields{
				GetItemDynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 10,
				},
				DeleteItemDynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 10,
				},
				PutItemDynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 10,
				},
				Retries: -2,
			},
			args: args{
				ctx:             ctx,
				getItemInput:    &ddb.GetItemInput{},
				deleteItemInput: &ddb.DeleteItemInput{},
				putItemInput:    &ddb.PutItemInput{},
			},
			wantGetItemOutput:    nil,
			wantDeleteItemOutput: nil,
			wantPutItemOutput:    nil,
			wantErr:              NewInvalidRetryError(-2),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// GetItem tests
			getItemClient := &RetryDynamoDBClient{
				DynamoDBClient: tt.fields.GetItemDynamoDBClient,
				Retries:        tt.fields.Retries,
				BackOffTime:    tt.fields.BackOffTime,
			}

			gotGetItemOutput, err := getItemClient.GetItem(tt.args.ctx, tt.args.getItemInput, tt.args.o...)
			assert.Equal(t, tt.wantGetItemOutput, gotGetItemOutput)
			assert.Equal(t, tt.wantErr, err)

			// DeleteItem tests
			deleteItemClient := &RetryDynamoDBClient{
				DynamoDBClient: tt.fields.DeleteItemDynamoDBClient,
				Retries:        tt.fields.Retries,
				BackOffTime:    tt.fields.BackOffTime,
			}

			gotDeleteItemOutput, err := deleteItemClient.DeleteItem(tt.args.ctx, tt.args.deleteItemInput, tt.args.o...)
			assert.Equal(t, tt.wantDeleteItemOutput, gotDeleteItemOutput)
			assert.Equal(t, tt.wantErr, err)

			// PutItem tests
			putItemClient := &RetryDynamoDBClient{
				DynamoDBClient: tt.fields.PutItemDynamoDBClient,
				Retries:        tt.fields.Retries,
				BackOffTime:    tt.fields.BackOffTime,
			}

			gotPutItemOutput, err := putItemClient.PutItem(tt.args.ctx, tt.args.putItemInput, tt.args.o...)
			assert.Equal(t, tt.wantPutItemOutput, gotPutItemOutput)
			assert.Equal(t, tt.wantErr, err)
		})
	}
}
