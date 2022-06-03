package db

import (
	"context"
	"encoding/json"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
)

type aerospikeClient struct {
	cl *as.Client
}

func newAerospikeClient(dbUrl string, dbPort int) AerospikeConnector {
	cl, err := as.NewClient(dbUrl, dbPort)
	if err != nil {
		panic(err)
	}

	return &aerospikeClient{cl: cl}
}


func (aerospike *aerospikeClient) Connect() error {
	ping := aerospike.cl.IsConnected()
	if ping{
		return fmt.Errorf("Not Connected to Aerospike")
	}
	return nil
}

func (aerospike *aerospikeClient) FindOne(ctx context.Context, ns string, set string, key string, bin string) (interface{}, error) {
	hashKey, err := as.NewKey(ns, set, key)
	if err != nil {
		return nil, err
	}

	val, err := aerospike.cl.Get(as.NewPolicy(), hashKey, bin)
	if err != nil{
		return nil, err
	}
	return val.Bins[bin], nil
}

func (aerospike *aerospikeClient) FindMany(ctx context.Context, ns string, set string, key string) (interface{}, error) {
	hashKey, err := as.NewKey(ns, set, key)
	if err != nil {
		return nil, err
	}
	val, err := aerospike.cl.Get(nil, hashKey)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (aerospike *aerospikeClient) Insert(ctx context.Context, ns string, set string, key string, document interface{}) (interface{}, error) {
	hashKey, err := as.NewKey(ns, set, key)
	if err != nil {
		return nil, err
	}

	bins:=binMap(document)

	result := aerospike.cl.Put(nil, hashKey, bins)
	if result == nil{
		return result, nil
	}

	return nil, fmt.Errorf("Could not insert into aersospike")
}


func (aerospike *aerospikeClient) Update(ctx context.Context, ns string, set string, key string, update interface{}) (interface{}, error){
	hashKey, err := as.NewKey(ns, set, key)
	if err != nil {
		return nil, err
	}
	policy := as.NewWritePolicy(0, 0)
	policy.RecordExistsAction = as.UPDATE

	bins:=binMap(update)

	result := aerospike.cl.Put(policy, hashKey, bins)
	if result==nil {
		return result, nil
	}
	return nil, fmt.Errorf("Could not insert into aersospike")
}

func (aerospike *aerospikeClient) UpdateInsertQuery(QueryPolicy *as.QueryPolicy, WritePolicy *as.WritePolicy, statements *as.Statement, operations *as.Operation) (*as.ExecuteTask, error){
	result, err := aerospike.cl.QueryExecute(QueryPolicy, WritePolicy, statements, operations)
	return result, err
}

func (aerospike *aerospikeClient) ReadQuery(QueryPolicy *as.QueryPolicy, statements *as.Statement) (*as.Recordset, error){
	result, err := aerospike.cl.Query(QueryPolicy, statements)
	return result, err
} 

func (aerospike *aerospikeClient) Cancel() error {
	client := aerospike.cl
	if client == nil {
		return nil
	}
	client.Close()
	fmt.Println("Connection to aerospike closed.")
	return nil
}

func binMap(in interface{})(map[string]interface{}){
	var bins map[string]interface{}
	json.Unmarshal([]byte(in.(string)), &bins)
	return bins
}
