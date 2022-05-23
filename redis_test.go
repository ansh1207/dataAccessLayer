package main

import (
	"context"
	"datalayer/db"
	"testing"
	"time"
)

type RedisInsertDoc struct {
	Key string
	Doc interface{}
	Expiry time.Duration	
}

type RedisFindDoc struct {
    Key []string
}

type RedisConfigType struct {
	DbType int
	DbUrl string
}

func Test_redisClient_InsertOne(t *testing.T) {

	var redisTestingClient = db.NewStore(RedisConfigType{DbType: 2, DbUrl: "localhost:6999"})
	redisTestingClient.Connect()
	defer redisTestingClient.Cancel()

	expired := time.Duration(600 * time.Second)
	type args struct {
		ctx        context.Context
		collection string
		document   RedisInsertDoc
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "case 1",
			args: args{ctx: context.TODO(), collection: "testCol1", document: RedisInsertDoc{Key: "testing", Doc: "testing the data", Expiry: expired} },
			want:    [12]byte{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := redisTestingClient.InsertOne(tt.args.ctx, tt.args.collection, tt.args.document)
			if (err != nil) != tt.wantErr {
				t.Errorf("redisClient.InsertOne() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_redisClient_InsertMany(t *testing.T) {

	var redisTestingClient = db.NewStore(RedisConfigType{DbType: 2, DbUrl: "localhost:6999"})
	redisTestingClient.Connect()
	defer redisTestingClient.Cancel()

	expired := time.Duration(600 * time.Second)
	type args struct {
		ctx        context.Context
		collection string
		document   []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "case 1",
			args: args{ctx: context.TODO(), collection: "testCol1", document: []interface{}{
				RedisInsertDoc{Key: "testing1", Doc: "testing the data 1", Expiry: expired},
				RedisInsertDoc{Key: "testing2", Doc: "testing the data 2", Expiry: expired},
			} },
			want:    [12]byte{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := redisTestingClient.InsertMany(tt.args.ctx, tt.args.collection, tt.args.document)
			if (err != nil) != tt.wantErr {
				t.Errorf("redisClient.InsertMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_redisClient_FindOne(t *testing.T) {

	var redisTestingClient = db.NewStore(RedisConfigType{DbType: 2, DbUrl: "localhost:6999"})
	redisTestingClient.Connect()
	defer redisTestingClient.Cancel()

	type args struct {
		ctx        context.Context
		collection string
		document   string
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "case 1",
			args: args{ctx: context.TODO(), collection: "testCol1", document: "testing2" },
			want:    [12]byte{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := redisTestingClient.FindOne(tt.args.ctx, tt.args.collection, tt.args.document)
			if (err != nil) != tt.wantErr {
				t.Errorf("redisClient.FindOne() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_redisClient_FindMany(t *testing.T) {

	var redisTestingClient = db.NewStore(RedisConfigType{DbType: 2, DbUrl: "localhost:6999"})
	redisTestingClient.Connect()
	defer redisTestingClient.Cancel()

	type args struct {
		ctx        context.Context
		collection string
		document   RedisFindDoc
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "case 1",
			args: args{ctx: context.TODO(), collection: "testCol1", document: RedisFindDoc{Key: []string{"testing1", "testing2"}} },
			want:    [12]byte{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := redisTestingClient.FindMany(tt.args.ctx, tt.args.collection, tt.args.document)
			if (err != nil) != tt.wantErr {
				t.Errorf("redisClient.FindMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}