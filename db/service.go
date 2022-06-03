// provides an interface for the accessing multiple databases
package db

import (
	"context"

	"github.com/mitchellh/mapstructure"
	as "github.com/aerospike/aerospike-client-go"
)

type StorageType int

const (
    mongoDB StorageType = 1 << iota
    redisDB
	aerospikeDB StorageType = 3
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
	UpdateMany(context.Context,string, interface{}, interface{}) (interface{}, error)
	Cancel() error
}

type AerospikeConnector interface {
	Connect() error
	FindOne(context.Context, string, string, string, string) (interface{}, error)
	FindMany(context.Context, string, string, string) (interface{}, error)
	Insert(context.Context, string, string, string, interface{}) (interface{}, error)
	Update(context.Context, string, string, string, interface{}) (interface{}, error)
	ReadQuery(*as.QueryPolicy, *as.Statement) (*as.Recordset, error)
	UpdateInsertQuery(*as.QueryPolicy, *as.WritePolicy, *as.Statement, *as.Operation) (*as.ExecuteTask, error)
	Cancel() error
}

type SingleResultHelper interface {
	Decode(v interface{}) error
}


type DbConfig struct {
    DbType StorageType
    DbUrl string
    DbName string
	DbPort int
}

func NewStore(config interface{}) DbConnector {
	configDoc := DbConfig{}
	mapstructure.Decode(config, &configDoc)

    switch configDoc.DbType {
    case mongoDB:
		return newMongoClient(configDoc.DbUrl, configDoc.DbName)
    case redisDB:
		return newRedisClient(configDoc.DbUrl)
	}
	return nil
}

func NewAerospikeStore(config interface{}) AerospikeConnector{
	configDoc := DbConfig{}
	mapstructure.Decode(config, &configDoc)
	if configDoc.DbType == 3{
		
	}
	return newAerospikeClient(configDoc.DbUrl, configDoc.DbPort)
}