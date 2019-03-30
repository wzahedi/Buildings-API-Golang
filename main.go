// Wahhaj Zahedi
// Topos Backend Engineering Assignment

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strconv"
)

// Building ...:
// ID: Building Identifier Number (BIN)
// ConstructYr: Year of construction completion
// Height: Height of building measured from ground to roof
// Area: Area of building polygon
// Type: Type of building represented by feature code
type Building struct {
	ID          string `json:"bin" bson:"id"`
	ConstructYr string `json:"cnstrct_yr" bson:"construct_yr"`
	Height      string `json:"heightroof" bson:"height"`
	Area        string `json:"shape_area" bson:"area"`
	Type        string `json:"feat_code" bson:"type"`
}

func main() {
	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), "mongodb://localhost:27017")
	if err != nil {
		log.Fatal(err)
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB.")
	collection := client.Database("ToposBuildings").Collection("buildings")

	// If detected no documents already exist, then run ETL
	count, err := collection.EstimatedDocumentCount(context.TODO())
	if err != nil {
		fmt.Printf("Count error: %s\n", err)
		return
	}
	if count == 0 {
		response, err := http.Get("https://data.cityofnewyork.us/resource/k8ez-gyqp.json")
		if err != nil {
			fmt.Printf("The HTTP request failed with error %s\n", err)
			return
		}
		data, _ := ioutil.ReadAll(response.Body)
		var buildings []Building
		json.Unmarshal(data, &buildings)
		var ui []interface{}
		for _, t := range buildings {
			ui = append(ui, t)
		}
		result, err := collection.InsertMany(context.TODO(), ui)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Inserted documents: ", result.InsertedIDs)
	}
	// HTTP server setup with API routes:
	http.HandleFunc("/", GetHome)
	http.HandleFunc("/all", GetBuildings(collection))
	http.HandleFunc("/building", GetBuildingByID(collection))
	http.HandleFunc("/smallerthan", GetLessThanHeight(collection))
	http.HandleFunc("/byyear", GetByConstructYr(collection))
	http.HandleFunc("/groupyear", GroupByYear(collection))
	http.HandleFunc("/data", CalculateData(collection))
	http.ListenAndServe(":80", nil)
}

//GetHome ... Default Page
func GetHome(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Wahhaj Zahedi\nTopos Backend Engineering Assignment")
}

//GetBuildings ... Get list of buildings
func GetBuildings(c *mongo.Collection) http.HandlerFunc {
	var items []Building
	fn := func(w http.ResponseWriter, r *http.Request) {
		cur, err := c.Find(context.TODO(), bson.M{})
		if err != nil {
			log.Fatal(err)
		}
		for cur.Next(context.TODO()) {
			var elem Building
			err := cur.Decode(&elem)
			if err != nil {
				log.Fatal(err)
			}
			items = append(items, elem)
		}
		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
		cur.Close(context.TODO())
		json.NewEncoder(w).Encode(items)
	}
	return http.HandlerFunc(fn)
}

//GetBuildingByID ... Get a building by its ID (BIN)
func GetBuildingByID(c *mongo.Collection) http.HandlerFunc {
	var result Building
	fn := func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		err := c.FindOne(context.TODO(), bson.M{"id": id}).Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		json.NewEncoder(w).Encode(result)
	}
	return http.HandlerFunc(fn)
}

//GetByConstructYr ... Get list of buildings by year
func GetByConstructYr(c *mongo.Collection) http.HandlerFunc {
	var results []*Building
	fn := func(w http.ResponseWriter, r *http.Request) {
		yr := r.URL.Query().Get("year")
		cur, err := c.Find(context.TODO(), bson.M{"construct_yr": yr})
		if err != nil {
			log.Fatal(err)
		}
		for cur.Next(context.TODO()) {
			var elem Building
			err := cur.Decode(&elem)
			if err != nil {
				log.Fatal(err)
			}
			results = append(results, &elem)
		}
		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
		cur.Close(context.TODO())
		json.NewEncoder(w).Encode(results)
	}
	return http.HandlerFunc(fn)
}

//GetLessThanHeight ... Get all buildings smaller than a certain height, sorted by decreasing height
func GetLessThanHeight(c *mongo.Collection) http.HandlerFunc {
	var results []*Building
	fn := func(w http.ResponseWriter, r *http.Request) {
		heightStr := r.URL.Query().Get("height")
		height, err := strconv.ParseFloat(heightStr, 64)
		if err != nil {
			fmt.Println(err)
		}
		cur, err := c.Find(context.TODO(), bson.M{})
		if err != nil {
			log.Fatal(err)
		}
		for cur.Next(context.TODO()) {
			var elem Building
			err := cur.Decode(&elem)
			if err != nil {
				log.Fatal(err)
			}
			itemHeight, err := strconv.ParseFloat(elem.Height, 64)
			if err != nil {
				fmt.Println(err)
			}
			if itemHeight <= height {
				results = append(results, &elem)
			}
		}
		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
		cur.Close(context.TODO())
		sort.Slice(results, func(i, j int) bool {
			h1, err := strconv.ParseFloat(results[i].Height, 64)
			if err != nil {
				fmt.Println(err)
			}
			h2, err := strconv.ParseFloat(results[j].Height, 64)
			if err != nil {
				fmt.Println(err)
			}
			return h1 > h2
		})
		json.NewEncoder(w).Encode(results)
	}
	return http.HandlerFunc(fn)
}

//CalculateData ... Returns a few selected calculations
func CalculateData(c *mongo.Collection) http.HandlerFunc {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var sum float64
		var heightSum float64
		var count int
		cur, err := c.Find(context.TODO(), bson.M{})
		if err != nil {
			log.Fatal(err)
		}
		for cur.Next(context.TODO()) {
			var elem Building
			err := cur.Decode(&elem)
			if err != nil {
				log.Fatal(err)
			}
			area, err := strconv.ParseFloat(elem.Area, 64)
			if err != nil {
				fmt.Println(err)
			}
			height, err := strconv.ParseFloat(elem.Height, 64)
			if err != nil {
				fmt.Println(err)
			}
			sum += area
			heightSum += height
			count++
		}
		avgHeight := heightSum / float64(count)
		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
		cur.Close(context.TODO())
		json.NewEncoder(w).Encode(bson.M{"totalArea": sum, "avgHeight": avgHeight, "buildingCount": count})
	}
	return http.HandlerFunc(fn)
}

//GroupByYear ... Aggregate building ID's by year and also return count
func GroupByYear(c *mongo.Collection) http.HandlerFunc {
	response := bson.A{}
	fn := func(w http.ResponseWriter, r *http.Request) {
		years, err := c.Distinct(context.TODO(), "construct_yr", bson.D{})
		if err != nil {
			fmt.Println(err)
		}
		for _, year := range years {
			group := []bson.M{{"$match": bson.M{"construct_yr": year}}}
			cur, err := c.Aggregate(
				context.TODO(),
				group,
			)
			if err != nil {
				fmt.Println(err)
			}
			var items []string
			for cur.Next(context.TODO()) {
				var elem Building
				err := cur.Decode(&elem)
				if err != nil {
					log.Fatal(err)
				}
				items = append(items, elem.ID)
			}
			cur.Close(context.TODO())
			response = append(response, bson.M{"construct_yr": year, "count": len(items), "buildings": items})
		}
		json.NewEncoder(w).Encode(response)
		response = nil
	}
	return http.HandlerFunc(fn)
}
