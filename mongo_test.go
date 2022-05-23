package main

import (
	"context"
	"datalayer/db"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type MongoConfigType struct {
	DbType int
	DbUrl string
	DbName string
}

func Test_mongoClient_InsertOne(t *testing.T) {
	var mongoTestingClient = db.NewStore(MongoConfigType{DbType: 1, DbUrl: "mongodb://localhost:27017", DbName: "tester"})
	mongoTestingClient.Connect()
	defer mongoTestingClient.Cancel()

	type args struct {
		ctx        context.Context
		collection string
		filter     interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    primitive.ObjectID
		wantErr bool
	}{
		{
			name: "case 1",
			args: args{ctx: context.TODO(), collection: "testCol", filter: bson.D{{"title", "Record of a Shriveled Datum"}, {"text", "No bytes, no problem. Just insert a document, in MongoDB"}, {"index", 1}}},
			want:    [12]byte{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mongoTestingClient.InsertOne(tt.args.ctx, tt.args.collection, tt.args.filter)
			if (err != nil) != tt.wantErr {
				t.Errorf("mongoClient.InsertOne() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

		})
	}
}

func Test_mongoClient_InsertMany(t *testing.T) {
	var mongoTestingClient = db.NewStore(MongoConfigType{DbType: 1, DbUrl: "mongodb://localhost:27017", DbName: "tester"})
	mongoTestingClient.Connect()
	defer mongoTestingClient.Cancel()
	type args struct {
		ctx        context.Context
		collection string
		filter     []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    [][12]byte
		wantErr bool
	}{
		{
			name: "case 1",
			args: args{ctx: context.TODO(), collection: "testCol", filter: []interface{}{
				bson.D{{"title", "Record of a Shriveled Datum"}, {"text", "No bytes, no problem. Just insert a document, in MongoDB"}, {"index", 2}},
				bson.D{{"title", "Showcasing a Blossoming Binary"}, {"text", "Binary data, safely stored with GridFS. Bucket the data"}, {"index", 3}},
				},
			},
			wantErr: false,
		},

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []interface{}
			got, err := mongoTestingClient.InsertMany(tt.args.ctx, tt.args.collection, tt.args.filter)
			if (err != nil) != tt.wantErr {
				t.Errorf("mongoClient.FindMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != 2 {
				t.Errorf("Inserted length is not corrent")
			}
		})
	}
}

func Test_mongoClient_FindMany(t *testing.T) {
	var mongoTestingClient = db.NewStore(MongoConfigType{DbType: 1, DbUrl: "mongodb://localhost:27017", DbName: "tester"})
	mongoTestingClient.Connect()
	defer mongoTestingClient.Cancel()
	type args struct {
		ctx        context.Context
		collection string
		filter     interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []interface{}
		wantErr bool
	}{
		{
			name: "case 1",
			args: args{ctx: context.TODO(), collection: "testCol", filter: bson.M{}},
			wantErr: false,
		},

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mongoTestingClient.FindMany(tt.args.ctx, tt.args.collection, tt.args.filter)
			if (err != nil) != tt.wantErr {
				t.Errorf("mongoClient.FindMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) == 0 {
				t.Errorf("mongoClient.FindMany() should not responde with empty map")
			}
		})
	}
}

func Test_mongoClient_FindOne(t *testing.T) {
	var mongoTestingClient = db.NewStore(MongoConfigType{DbType: 1, DbUrl: "mongodb://localhost:27017", DbName: "tester"})
	mongoTestingClient.Connect()
 	defer mongoTestingClient.Cancel()

	type args struct {
		ctx        context.Context
		collection string
		document   interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "case 1",
			args: args{ctx: context.TODO(), collection: "testCol", document: bson.M{}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mongoTestingClient.FindOne(tt.args.ctx, tt.args.collection, tt.args.document)
			if (err != nil) != tt.wantErr {
				t.Errorf("mongoClient.FindOne() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}


func Test_mongoClient_UpdateOne(t *testing.T) {
	var mongoTestingClient = db.NewStore(MongoConfigType{DbType: 1, DbUrl: "mongodb://localhost:27017", DbName: "tester"})
	mongoTestingClient.Connect()
 	defer mongoTestingClient.Cancel()

	type args struct {
		ctx        context.Context
		collection string
		filter     interface{}
		update     interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "case 1",
			args: args{ctx: context.TODO(), collection: "testCol", filter: bson.M{"index": 1}, update: bson.D{ {"$set", bson.D{{"text", "things have changed now !!"}} }} },
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mongoTestingClient.UpdateOne(tt.args.ctx, tt.args.collection, tt.args.filter, tt.args.update)
			if (err != nil) != tt.wantErr {
				t.Errorf("mongoClient.UpdateOne() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_mongoClient_UpdateMany(t *testing.T) {
	var mongoTestingClient = db.NewStore(MongoConfigType{DbType: 1, DbUrl: "mongodb://localhost:27017", DbName: "tester"})
	mongoTestingClient.Connect()
 	defer mongoTestingClient.Cancel()

	type args struct {
		ctx        context.Context
		collection string
		filter     interface{}
		update     interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "case 1",
			args: args{ctx: context.TODO(), collection: "testCol", filter: bson.D{},update: bson.D{{"$set", bson.D{{"text", "things have changed now !!"}}}} },
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mongoTestingClient.UpdateMany(tt.args.ctx, tt.args.collection, tt.args.filter, tt.args.update)
			if (err != nil) != tt.wantErr {
				t.Errorf("mongoClient.UpdateMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
