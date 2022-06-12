// provides an interface for the accessing multiple databases
package db

import (
	"context"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/mitchellh/mapstructure"
)

type StorageType int

const (
	mongoDB StorageType = 1 << iota
	redisDB
	aerospikeDB
)

/*
The functions that are exposed to be used by multiple databases
*/
type DbConnector interface {
	Connect() error
	FindOne(context.Context, string, interface{}) (interface{}, error)
	FindMany(context.Context, string, interface{}) ([]interface{}, error)
	InsertOne(context.Context, string, interface{}) (interface{}, error)
	InsertMany(context.Context, string, []interface{}) ([]interface{}, error)
	UpdateOne(context.Context, string, interface{}, interface{}) (interface{}, error)
	UpdateMany(context.Context, string, interface{}, interface{}) (interface{}, error)
	Cancel() error
}

type AerospikeConnector interface {
	Connect() error
	FindOne(context.Context, string, string, string, string) (interface{}, error)
	FindMany(context.Context, string, string, string) (interface{}, error)
	Insert(context.Context, string, string, string, interface{}) (interface{}, error)
	Update(context.Context, string, string, string, interface{}) (interface{}, error)
	ReadQuery(context.Context, *as.QueryPolicy, *as.Statement) (*as.Recordset, error)
	UpdateInsertQuery(context.Context, *as.QueryPolicy, *as.WritePolicy, *as.Statement, *as.Operation) (*as.ExecuteTask, error)
	Cancel() error
}

type ReddisConnector interface {
	Connect() error
	FindOne(context.Context, string, interface{}) (interface{}, error)
	FindMany(context.Context, string, interface{}) ([]interface{}, error)
	FindOneHash(context.Context, string, interface{}, string) (interface{}, error)
	FindManyHash(context.Context, []interface{}) ([]interface{}, error)
	InsertOne(context.Context, string, interface{}) (interface{}, error)
	InsertMany(context.Context, string, []interface{}) ([]interface{}, error)
	InsertOneHash(context.Context, string, interface{}) (interface{}, error)
	InsertManyHash(context.Context, []interface{}) (interface{}, error)
	Cancel() error
}

type SingleResultHelper interface {
	Decode(v interface{}) error
}

type DbConfig struct {
	DbType        StorageType
	DbUrl         string
	DbName        string
	DbPort        int
	NewConnection bool
}

func NewStore(config interface{}) DbConnector {
	configDoc := DbConfig{}
	mapstructure.Decode(config, &configDoc)

	switch configDoc.DbType {
	case mongoDB:
		return newMongoClient(configDoc.DbUrl, configDoc.DbName)
	}
	return nil
}

func NewAerospikeStore(config interface{}) AerospikeConnector {
	configDoc := DbConfig{}
	mapstructure.Decode(config, &configDoc)
	if configDoc.DbType == aerospikeDB {
		return newAerospikeClient(configDoc.DbUrl, configDoc.DbPort, configDoc.NewConnection)
	}
	return nil
}

func NewReddisStore(config interface{}) ReddisConnector {
	configDoc := DbConfig{}
	mapstructure.Decode(config, &configDoc)
	if configDoc.DbType == redisDB {
		return newRedisClient(configDoc.DbUrl)
	}
	return nil
}
