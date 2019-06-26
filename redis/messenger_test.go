package redis

import (
	"context"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	client "github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestPoolMessenger(t *testing.T) {
	m := PoolMessenger{
		Pool: &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", ":6379")
			},
		},
	}
	channel := "test_channel_" + uuid.New().String()[:4]
	message := "test_message"
	go func() {
		time.Sleep(time.Second * 2)
		err := m.Publish(context.Background(), channel, message)
		assert.NoError(t, err)
	}()

	replies, errs := m.Subscribe(context.Background(), channel)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	select {
	case r := <-replies:
		assert.Equal(t, message, r)
	case err := <-errs:
		t.Error(err)
	case <-ctx.Done():
		t.Fatal(context.DeadlineExceeded)
	}
}

func TestPoolMessenger_Publish(t *testing.T) {
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", ":6379")
		},
	}
	type fields struct {
		Pool *client.Pool
	}
	type args struct {
		ctx     context.Context
		channel string
		msg     string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		err     error
	}{
		{
			name: "Publish should return ErrInvalidChannel",
			fields: fields{
				Pool: pool,
			},
			args: args{
				ctx:     context.Background(),
				channel: "",
				msg:     "message",
			},
			wantErr: true,
			err:     ErrInvalidChannel,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := PoolMessenger{
				Pool: tt.fields.Pool,
			}
			if err := m.Publish(tt.args.ctx, tt.args.channel, tt.args.msg); ((err != nil) != tt.wantErr) != (tt.err != err) {
				t.Errorf("PoolMessenger.Publish() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPoolMessenger_Subscribe(t *testing.T) {
	m := PoolMessenger{
		Pool: &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", ":6379")
			},
		},
	}
	_, err := m.Subscribe(context.Background(), "")
	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*500)
	select {
	case e := <-err:
		assert.Equal(t, ErrInvalidChannel, e)
	case <-ctx.Done():
		t.Fatal(context.DeadlineExceeded)
	}
}
