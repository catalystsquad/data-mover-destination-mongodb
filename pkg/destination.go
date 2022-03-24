package pkg

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

type MongoDBDestination struct {
	Uri                     string
	ConnectionTimeoutString string
	ConnectionTimeout       time.Duration
	QueryTimeoutString      string
	QueryTimeout            time.Duration
	DatabaseName            string
	CollectionName          string
	Client                  *mongo.Client
	Collection              *mongo.Collection
}

func (d *MongoDBDestination) Initialize() error {
	var err error
	d.ConnectionTimeout, err = time.ParseDuration(d.ConnectionTimeoutString)
	if err != nil {
		return err
	}
	d.QueryTimeout, err = time.ParseDuration(d.QueryTimeoutString)
	if err != nil {
		return err
	}

	connectionContext, cancel := context.WithTimeout(context.Background(), d.ConnectionTimeout)
	defer cancel()

	// connect
	d.Client, err = mongo.Connect(connectionContext, options.Client().ApplyURI(d.Uri))
	if err != nil {
		return err
	}

	// Ping the primary
	ctx, cancel := context.WithTimeout(context.Background(), d.QueryTimeout)
	defer cancel()
	err = d.Client.Ping(ctx, readpref.Primary())
	if err != nil {
		return err
	}

	// set the collection
	d.Collection = d.Client.Database(d.DatabaseName).Collection(d.CollectionName)

	return nil
}

func (d *MongoDBDestination) Persist(data []map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), d.QueryTimeout)
	defer cancel()
	docs, err := toDocs(data)
	if err != nil {
		return err
	}
	_, err = d.Collection.InsertMany(ctx, docs)
	return err
}

func toDocs(data []map[string]interface{}) (docs []interface{}, err error) {
	for _, item := range data {
		doc, err := toDoc(item)
		if err != nil {
			return
		}
		docs = append(docs, doc)
	}
	return
}

func toDoc(v interface{}) (doc *bson.D, err error) {
	data, err := bson.Marshal(v)
	if err != nil {
		return
	}

	err = bson.Unmarshal(data, &doc)
	return
}
