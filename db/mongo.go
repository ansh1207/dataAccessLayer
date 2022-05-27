package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type mongoClient struct {
	cl *mongo.Client
	db *mongo.Database
}

type mongoSingleResult struct {
	sr *mongo.SingleResult
}

func newMongoClient(dbUrl string, dbName string) DbConnector {
	cl, err := mongo.NewClient(options.Client().ApplyURI(dbUrl))
	if err != nil {
		panic(err)
	}
	db := cl.Database(dbName)
	return &mongoClient{cl: cl, db: db}
}

func (mc *mongoClient) Connect() error {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err := mc.cl.Connect(ctx)
	if err != nil {
		return err
	}
	err = mongoPing(mc.cl, ctx)
	if err != nil {
		return err
	}
	return nil
}

func mongoPing(client *mongo.Client, ctx context.Context) error {
	err := client.Ping(ctx, readpref.Primary())
	if err != nil {
		return err
	}
	fmt.Println("mongo connected successfully !!")
	return nil
}

func (mc *mongoClient) FindOne(ctx context.Context, collection string, filter interface{}) (interface{}, error) {
	singleResult := mc.db.Collection(collection).FindOne(ctx, filter)
	var singleDoc bson.M
	if err := singleResult.Decode(&singleDoc); err != nil {
		log.Fatal(err)
	}
	return singleDoc, nil
}

func (mc *mongoClient) FindOneHash(context.Context, string, interface{}, string) (interface{}, error) {
	return nil, nil
}

func (mc *mongoClient) FindMany(ctx context.Context, collection string, filter interface{}) ([]interface{}, error) {
	result, err := mc.db.Collection(collection).Find(ctx, filter)
	if err != nil {
		panic(err)
	}
	var resultMap []interface{}
	if err = result.All(ctx, &resultMap); err != nil {
		log.Fatal(err)
	}
	return resultMap, err
}

func (mc *mongoClient) InsertOne(ctx context.Context, collection string, document interface{}) (interface{}, error) {
	result, err := mc.db.Collection(collection).InsertOne(ctx, document)
	return result.InsertedID, err
}

func (mc *mongoClient) InsertMany(ctx context.Context, collection string, document []interface{}) ([]interface{}, error) {
	result, err := mc.db.Collection(collection).InsertMany(ctx, document)
	return result.InsertedIDs, err
}

func (mc *mongoClient) UpdateOne(ctx context.Context, collection string, filter interface{}, update interface{}) (interface{}, error) {
	result, err := mc.db.Collection(collection).UpdateOne(ctx, filter, update)
	return result, err
}

func (mc *mongoClient) UpdateMany(ctx context.Context, collection string, filter interface{}, update interface{}) (interface{}, error) {
	result, err := mc.db.Collection(collection).UpdateMany(ctx, filter, update)
	return result, err
}

func (sr *mongoSingleResult) Decode(v interface{}) error {
	return sr.sr.Decode(v)
}

func (mc *mongoClient) Cancel() error {
	client := mc.cl
	if client == nil {
		return nil
	}

	err := client.Disconnect(context.TODO())
	if err != nil {
		panic(err)
	}
	fmt.Println("Connection to MongoDB closed.")
	return nil
}
