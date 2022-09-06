package pkg

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoDBDestination struct {
	Uri                     string
	ConnectionTimeoutString string
	ConnectionTimeout       time.Duration
	QueryTimeoutString      string
	QueryTimeout            time.Duration
	DatabaseName            string
	CollectionName          string
	UniqueIDFieldName       string
	Client                  *mongo.Client
	Collection              *mongo.Collection
}

func NewMongoDBDestination(uri, connectionTimeout, queryTimeout, database, collection, uniqueIDFieldName string) *MongoDBDestination {
	return &MongoDBDestination{
		Uri:                     uri,
		ConnectionTimeoutString: connectionTimeout,
		QueryTimeoutString:      queryTimeout,
		DatabaseName:            database,
		CollectionName:          collection,
		UniqueIDFieldName:       uniqueIDFieldName,
	}
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

	// set a unique ID field index on the collection
	_, err = d.Collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: d.UniqueIDFieldName, Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	return err
}

func (d *MongoDBDestination) Persist(data []map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), d.QueryTimeout)
	defer cancel()

	for _, item := range data {
		setItem := map[string]interface{}{"$set": item}
		var doc interface{}
		doc, err := toDoc(setItem)
		if err != nil {
			return err
		}
		filter := bson.M{d.UniqueIDFieldName: item[d.UniqueIDFieldName]}
		_, err = d.Collection.UpdateOne(ctx, filter, doc, options.Update().SetUpsert(true))
		if err != nil {
			return err
		}
	}
	return nil
}

func toDoc(v interface{}) (doc *bson.D, err error) {
	data, err := bson.Marshal(v)
	if err != nil {
		return
	}

	err = bson.Unmarshal(data, &doc)
	return
}
