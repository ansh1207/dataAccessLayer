package db

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/mitchellh/mapstructure"
)

type redisClient struct {
	cl *redis.Client
}

type RedisInsertDoc struct {
	Key    string
	Doc    interface{}
	Expiry time.Duration
}

type RedisInsertHash struct {
	Collection string
	Key        string
	Doc        interface{}
	Expiry     time.Duration
}

type RedisFindHash struct {
	Collection string
	Key        string
	Field      string
}

type RedisFindDoc struct {
	Key []string
}

var batchSize = 100

func newRedisClient(dbUrl string) ReddisConnector {
	cl := redis.NewClient(&redis.Options{
		Addr: dbUrl,
	})
	return &redisClient{cl: cl}
}

func (rc *redisClient) Connect() error {
	_, err := rc.cl.Ping(context.TODO()).Result()
	if err != nil {
		return err
	}
	return nil
}

func (rc *redisClient) FindOne(ctx context.Context, collection string, filter interface{}) (interface{}, error) {
	subKey := filter.(string)
	val, err := rc.cl.Get(context.TODO(), createRedisKey(collection, subKey)).Result()
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (rc *redisClient) FindOneHash(ctx context.Context, collection string, filter interface{}, field string) (interface{}, error) {
	subKey := filter.(string)
	val, err := rc.cl.HGet(context.TODO(), createRedisKey(collection, subKey), field).Result()
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (rc *redisClient) FindManyHash(ctx context.Context, document []interface{}) ([]interface{}, error) {

	resultChan := make(chan interface{})
	errChan := make(chan error)

	batches := createBatches(document)
	for i := range batches {
		go findBatchFromHash(rc, ctx, batches[i], errChan, resultChan)
	}

	results := <-resultChan
	return results.([]interface{}), nil
}

func findBatchFromHash(rc *redisClient, ctx context.Context, document []interface{}, errChan chan<- error, resultChan chan<- interface{}) {
	pipe := rc.cl.TxPipeline()
	var results []interface{}
	for i := range document {
		convertedDoc := RedisFindHash{}
		err := mapstructure.Decode(document[i], &convertedDoc)
		if err != nil {
			errChan <- err
		}
		val, err := rc.cl.HGet(context.TODO(), createRedisKey(convertedDoc.Collection, convertedDoc.Key), convertedDoc.Field).Result()
		if err != nil {
			errChan <- err
		}
		results = append(results, val)
	}

	if _, err := pipe.Exec(context.TODO()); err != nil {
		errChan <- err
	}

	resultChan <- results

}

func (rc *redisClient) InsertOneHash(ctx context.Context, collection string, document interface{}) (interface{}, error) {

	convertedDoc := RedisInsertHash{}
	err := mapstructure.Decode(document, &convertedDoc)
	if err != nil {
		return nil, err
	}
	if err := rc.cl.HMSet(ctx, convertedDoc.Collection, convertedDoc.Key, convertedDoc.Doc).Err(); err != nil {
		return nil, err
	}

	rc.cl.Expire(ctx, convertedDoc.Collection, convertedDoc.Expiry)

	return nil, nil
}

func (rc *redisClient) InsertManyHash(ctx context.Context, document []interface{}) (interface{}, error) {

	errChan := make(chan error)
	batches := createBatches(document)
	for i := range batches {
		go insertBatchToHash(rc, ctx, batches[i], errChan)
	}
	errors := <-errChan
	return nil, errors
}

func insertBatchToHash(rc *redisClient, ctx context.Context, document []interface{}, errChan chan<- error) {
	pipe := rc.cl.TxPipeline()

	for i := range document {
		convertedDoc := RedisInsertHash{}
		err := mapstructure.Decode(document[i], &convertedDoc)
		if err != nil {
			errChan <- err
		}
		if err := rc.cl.HMSet(ctx, convertedDoc.Collection, convertedDoc.Key, convertedDoc.Doc).Err(); err != nil {
			errChan <- err
		}

		pipe.Expire(ctx, convertedDoc.Collection, convertedDoc.Expiry)
	}
	if _, err := pipe.Exec(context.TODO()); err != nil {
		errChan <- err
	}

	close(errChan)
}

func (rc *redisClient) FindMany(ctx context.Context, collection string, filter interface{}) ([]interface{}, error) {
	convertedDoc := RedisFindDoc{}
	err := mapstructure.Decode(filter, &convertedDoc)
	if err != nil {
		return nil, err
	}

	for i, data := range convertedDoc.Key {
		convertedDoc.Key[i] = createRedisKey(collection, data)
	}
	return rc.cl.MGet(context.TODO(), convertedDoc.Key...).Result()

}

func (rc *redisClient) InsertOne(ctx context.Context, collection string, document interface{}) (interface{}, error) {

	convertedDoc := RedisInsertDoc{}
	err := mapstructure.Decode(document, &convertedDoc)
	if err != nil {
		return nil, err
	}

	err = rc.cl.Set(context.TODO(), createRedisKey(collection, convertedDoc.Key), convertedDoc.Doc, time.Duration(convertedDoc.Expiry)).Err()
	var res interface{} = false
	if err != nil {
		return res, err
	}
	return nil, nil
}

func createRedisKey(collection string, subcollection string) string {
	if subcollection == "" {
		return collection
	} else {
		return collection + ":" + subcollection
	}
}

func (rc *redisClient) InsertMany(ctx context.Context, collection string, document []interface{}) ([]interface{}, error) {

	var ifaces []interface{}
	pipe := rc.cl.TxPipeline()

	for i := range document {
		convertedDoc := RedisInsertDoc{}
		err := mapstructure.Decode(document[i], &convertedDoc)
		if err != nil {
			return nil, err
		}

		ifaces = append(ifaces, createRedisKey(collection, convertedDoc.Key), convertedDoc.Doc)
		pipe.Expire(context.TODO(), createRedisKey(collection, convertedDoc.Key), convertedDoc.Expiry)
	}

	if err := rc.cl.MSet(context.TODO(), ifaces...).Err(); err != nil {
		return nil, err
	}
	if _, err := pipe.Exec(context.TODO()); err != nil {
		return nil, err
	}
	return nil, nil
}

func (rc *redisClient) Cancel() error {
	client := rc.cl
	if client == nil {
		return nil
	}
	err := client.Close()
	if err != nil {
		panic(err)
	}
	fmt.Println("Connection to redis closed.")
	return nil
}

func createBatches(data []interface{}) [][]interface{} {
	var batches [][]interface{}

	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize

		if end > len(data) {
			end = len(data)
		}

		batches = append(batches, data[i:end])
	}

	return batches
}
