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

func newRedisClient(dbUrl string) DbConnector {
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

type RedisFindDoc struct {
	Key []string
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

func (rc *redisClient) FindMany(ctx context.Context, collection string, filter interface{}) ([]interface{}, error) {
	convertedDoc := RedisFindDoc{}
	mapstructure.Decode(filter, &convertedDoc)

	for i, data := range convertedDoc.Key {
		convertedDoc.Key[i] = createRedisKey(collection, data)
	}
	return rc.cl.MGet(context.TODO(), convertedDoc.Key...).Result()

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

func (rc *redisClient) InsertOne(ctx context.Context, collection string, document interface{}) (interface{}, error) {

	convertedDoc := RedisInsertDoc{}
	mapstructure.Decode(document, &convertedDoc)

	err := rc.cl.Set(context.TODO(), createRedisKey(collection, convertedDoc.Key), convertedDoc.Doc, time.Duration(convertedDoc.Expiry)).Err()
	var res interface{} = false
	if err != nil {
		return res, err
	}
	return nil, nil
}

func (rc *redisClient) InsertOneHash(ctx context.Context, collection string, document ...interface{}) (interface{}, error) {
	// var ifaces []interface{}
	pipe := rc.cl.TxPipeline()

	for i := range document {
		convertedDoc := RedisInsertDoc{}
		mapstructure.Decode(document[i], &convertedDoc)
		if err := rc.cl.HMSet(context.TODO(), collection, convertedDoc.Key, convertedDoc.Doc).Err(); err != nil {
			return nil, err
		}
		// ifaces = append(ifaces, createRedisKey(collection, convertedDoc.Key), convertedDoc.Doc)
		pipe.Expire(context.TODO(), createRedisKey(collection, convertedDoc.Key), convertedDoc.Expiry)
	}

	if _, err := pipe.Exec(context.TODO()); err != nil {
		return nil, err
	}
	return nil, nil
}

func (rc *redisClient) InsertManyHash(ctx context.Context, document []interface{}) (interface{}, error) {
	// var ifaces []interface{}
	pipe := rc.cl.TxPipeline()

	for i := range document {
		convertedDoc := RedisInsertHash{}
		mapstructure.Decode(document[i], &convertedDoc)
		if err := rc.cl.HMSet(context.TODO(), convertedDoc.Collection, convertedDoc.Key, convertedDoc.Doc).Err(); err != nil {
			return nil, err
		}
		// ifaces = append(ifaces, createRedisKey(collection, convertedDoc.Key), convertedDoc.Doc)
		pipe.Expire(context.TODO(), convertedDoc.Collection, convertedDoc.Expiry)
	}

	if _, err := pipe.Exec(context.TODO()); err != nil {
		return nil, err
	}
	return nil, nil
}

func (rc *redisClient) FindManyHash(ctx context.Context, document []interface{}) ([]interface{}, error) {
	var results []interface{}
	pipe := rc.cl.TxPipeline()

	for i := range document {
		convertedDoc := RedisFindHash{}
		mapstructure.Decode(document[i], &convertedDoc)
		val, err := rc.cl.HGet(context.TODO(), createRedisKey(convertedDoc.Collection, convertedDoc.Key), convertedDoc.Field).Result()
		if err != nil {
			return nil, err
		}
		results = append(results, val)
	}

	if _, err := pipe.Exec(context.TODO()); err != nil {
		return nil, err
	}
	return results, nil
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
		mapstructure.Decode(document[i], &convertedDoc)

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

func (rc *redisClient) UpdateOne(ctx context.Context, collection string, filter interface{}, update interface{}) (interface{}, error) {
	return nil, nil
}

func (rc *redisClient) UpdateMany(ctx context.Context, collection string, filter interface{}, update interface{}) (interface{}, error) {
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
