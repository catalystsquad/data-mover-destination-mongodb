package test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/catalystsquad/data-mover-core/pkg"
	pkg2 "github.com/catalystsquad/data-mover-destination-mongodb/pkg"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/mongo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
)

var numIterations, executedIterations int64
var sourceData []map[string]interface{}
var numUpsertIterations, executedUpsertIterations int64
var upsertData []map[string]interface{}
var lock = new(sync.Mutex)
var source pkg.Source
var dest *pkg2.MongoDBDestination
var mongoContainer *gnomock.Container
var mongoUser, mongoPass = "test", "test"

type DestinationSuite struct {
	suite.Suite
}

func (s *DestinationSuite) SetupSuite() {
	var err error
	preset := mongo.Preset(
		// this could be removed to run without a user/pass, just update the uri as well
		mongo.WithUser(mongoUser, mongoPass),
	)
	mongoContainer, err = gnomock.Start(preset)
	require.NoError(s.T(), err)
}

func (s *DestinationSuite) TearDownSuite() {
	err := gnomock.Stop(mongoContainer)
	require.NoError(s.T(), err)
}

func (s *DestinationSuite) SetupTest() {
	addr := mongoContainer.DefaultAddress()
	uri := fmt.Sprintf("mongodb://%s:%s@%s", mongoUser, mongoPass, addr)
	// init source and destination
	source = TestSource{}
	dest = pkg2.NewMongoDBDestination(uri, "10s", "10s", "test", "test", "custom_id")
	err := dest.Initialize()
	assert.NoError(s.T(), err)
	// init variables
	numIterations = int64(gofakeit.Number(100, 200))
	executedIterations = 0
	sourceData = []map[string]interface{}{}
	numUpsertIterations = 10
	executedUpsertIterations = 0
	upsertData = []map[string]interface{}{}
	// drop test db before each test
	ctx, cancel := context.WithTimeout(context.Background(), dest.QueryTimeout)
	defer cancel()
	err = dest.Client.Database("test").Drop(ctx)
	assert.NoError(s.T(), err)
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(DestinationSuite))
}

func (s *DestinationSuite) TestConcurrentMove() {
	errorHandler := func(err error) bool {
		return true
	}
	mover, err := pkg.NewDataMover(10, 10, source, dest, errorHandler, errorHandler)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), mover)
	stats, err := mover.Move()
	assert.NoError(s.T(), err)
	assert.Len(s.T(), stats.SourceErrors, 0)
	assert.Len(s.T(), stats.DestinationErrors, 0)
	assert.Greater(s.T(), stats.Duration.Microseconds(), int64(0)) // this is very fast because it's a unit test, milliseconds comes out as 0, so using microseconds
	assert.Greater(s.T(), stats.RecordsPerSecond, float64(0))
	for _, sourceRecord := range sourceData {
		ctx, cancel := context.WithTimeout(context.Background(), dest.QueryTimeout)
		defer cancel()
		filter := bson.M{}
		for key, value := range sourceRecord {
			filter[key] = value
		}
		result := dest.Collection.FindOne(ctx, filter)
		assert.NoError(s.T(), result.Err())
		dbRecord := map[string]interface{}{}
		err = result.Decode(&dbRecord)
		assert.NoError(s.T(), err)
		// drop the id field
		delete(dbRecord, "_id")
		assert.Equal(s.T(), sourceRecord, dbRecord)
	}
	// test upsert on subset of data
	upsertSource := TestSource{TestUpsertData: true}
	upsertMover, err := pkg.NewDataMover(10, 10, upsertSource, dest, errorHandler, errorHandler)
	assert.NoError(s.T(), err)
	stats, err = upsertMover.Move()
	assert.NoError(s.T(), err)
	assert.Len(s.T(), stats.SourceErrors, 0)
	assert.Len(s.T(), stats.DestinationErrors, 0)
	assert.Greater(s.T(), stats.Duration.Microseconds(), int64(0))
	for _, sourceRecord := range upsertData {
		ctx, cancel := context.WithTimeout(context.Background(), dest.QueryTimeout)
		defer cancel()
		filter := bson.M{}
		for key, value := range sourceRecord {
			// simplify filter to only query by ID, so we know that our upsert
			// actually updated an existing record
			if key == "custom_id" {
				filter[key] = value
			}
		}
		cursor, err := dest.Collection.Find(ctx, filter)
		assert.NoError(s.T(), err)
		dbRecords := []map[string]interface{}{}
		for cursor.Next(context.TODO()) {
			var record map[string]interface{}
			err = cursor.Decode(&record)
			assert.NoError(s.T(), err)
			dbRecords = append(dbRecords, record)
		}
		assert.Len(s.T(), dbRecords, 1)
		dbRecord := dbRecords[0]
		// drop the id field
		delete(dbRecord, "_id")
		assert.Equal(s.T(), sourceRecord, dbRecord)
	}
}

type TestSource struct {
	TestUpsertData bool
}

func (t TestSource) Initialize() error {
	return nil
}

func (t TestSource) GetData() ([]map[string]interface{}, error) {
	data := []map[string]interface{}{}
	if t.TestUpsertData {
		if executedUpsertIterations < numUpsertIterations {
			record := sourceData[executedUpsertIterations]
			executedUpsertIterations++
			record[gofakeit.Name()] = gofakeit.HackerPhrase()
			data = append(data, record)
		}
		appendUpsertData(data)
	} else {
		if executedIterations < numIterations {
			executedIterations++
			numRecords := gofakeit.Number(1, 3)
			for i := 0; i < numRecords; i++ {
				numKeys := gofakeit.Number(1, 3)
				record := map[string]interface{}{}
				for i := 0; i < numKeys; i++ {
					record["custom_id"] = gofakeit.UUID()
					record[gofakeit.Name()] = gofakeit.HackerPhrase()
				}
				data = append(data, record)
			}
		}
		// append generated data to source data in a thread safe manner
		appendSourceData(data)
	}
	return data, nil
}

func appendSourceData(data []map[string]interface{}) {
	lock.Lock()
	defer lock.Unlock()
	sourceData = append(sourceData, data...)
}

func appendUpsertData(data []map[string]interface{}) {
	lock.Lock()
	defer lock.Unlock()
	upsertData = append(upsertData, data...)
}
