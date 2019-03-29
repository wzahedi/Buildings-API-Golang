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

var buildings []Building

func main() {
	// Mongo Connect
	client, err := mongo.Connect(context.TODO(), "mongodb://localhost:27017")
	if err != nil {
		log.Fatal(err)
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB!")
	collection := client.Database("ToposBuildings").Collection("buildings")
	fmt.Println("Connection to MongoDB established.")

	// Run ETL process if first run, else grab all the data from db for API usage
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
		json.Unmarshal(data, &buildings)
		var ui []interface{}
		for _, t := range buildings {
			ui = append(ui, t)
		}
		insertManyResult, err := collection.InsertMany(context.TODO(), ui)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Inserted multiple documents: ", insertManyResult.InsertedIDs)
	} else {
		cur, err := collection.Find(context.TODO(), bson.D{})
		if err != nil {
			log.Fatal(err)
		}
		for cur.Next(context.TODO()) {
			var elem Building
			err := cur.Decode(&elem)
			if err != nil {
				log.Fatal(err)
			}
			buildings = append(buildings, elem)
		}
		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
		cur.Close(context.TODO())
	}
	// HTTP server setup with API routes:
	http.HandleFunc("/", GetHome)
	http.HandleFunc("/all", GetBuildings)
	http.HandleFunc("/building", GetBuildingByID)
	http.HandleFunc("/smallerthan", GetLessThanHeight)
	http.HandleFunc("/byyear", GetByConstructYr)
	http.HandleFunc("/totalarea", calculateTotalArea)
	http.ListenAndServe(":80", nil)
}

//GetHome ... Landing Page
func GetHome(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Wahhaj Zahedi\nTopos Backend Engineering Assignment")
}

//GetBuildings ... Get list of buildings
func GetBuildings(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(buildings)
}

//GetBuildingByID ... Get a building by its ID (BIN)
func GetBuildingByID(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	for _, item := range buildings {
		if item.ID == id {
			json.NewEncoder(w).Encode(item)
			return
		}
	}
	json.NewEncoder(w).Encode(&Building{})
}

//GetByConstructYr ... Get list of buildings by year
func GetByConstructYr(w http.ResponseWriter, r *http.Request) {
	yr := r.URL.Query().Get("year")
	var items []Building
	for _, item := range buildings {
		if item.ConstructYr == yr {
			items = append(items, item)
		}
	}
	json.NewEncoder(w).Encode(items)
}

//GetLessThanHeight ... Get all buildings smaller than a certain height, sorted by decreasing height
func GetLessThanHeight(w http.ResponseWriter, r *http.Request) {
	height := r.URL.Query().Get("height")
	var items []Building
	i, err := strconv.ParseFloat(height, 64)
	if err != nil {
		fmt.Println(err)
	}
	for _, item := range buildings {
		j, err := strconv.ParseFloat(item.Height, 64)
		if err != nil {
			fmt.Println(err)
		}
		if j < i {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		h1, err := strconv.ParseFloat(items[i].Height, 64)
		if err != nil {
			fmt.Println(err)
		}
		h2, err := strconv.ParseFloat(items[j].Height, 64)
		if err != nil {
			fmt.Println(err)
		}
		return h1 > h2
	})
	json.NewEncoder(w).Encode(items)
}

//GetByConstructYr ... Returns the calculated area from all the buildings
func calculateTotalArea(w http.ResponseWriter, r *http.Request) {
	var sum float64
	for _, item := range buildings {
		area, err := strconv.ParseFloat(item.Area, 64)
		if err != nil {
			fmt.Println(err)
		}
		sum += area
	}
	fmt.Fprint(w, sum)
}
