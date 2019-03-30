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
	"strconv"
)

// Building ...:
// ID: Building Identifier Number (BIN)
// ConstructYr: Year of construction completion
// Height: Height of building measured from ground to roof
// Area: Area of building polygon
type Building struct {
	ID          int64   `json:"bin,string"`
	ConstructYr int16   `json:"cnstrct_yr,string"`
	Height      float64 `json:"heightroof,string"`
	Area        float64 `json:"shape_area,string"`
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
	collection := client.Database("WahhajTopos").Collection("buildings")

	// If detect no documents already exist, then do ETL
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
	fmt.Println("HTTP Server running on port 80.")
	http.ListenAndServe(":80", nil)
}

//GetHome ... Default Page
func GetHome(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Wahhaj Zahedi\nTopos Backend Engineering Assignment")
}

//GetBuildings ... Get list of all buildings
func GetBuildings(c *mongo.Collection) http.HandlerFunc {
	items := bson.A{}
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

			building := bson.M{"id": elem.ID, "constructyr": elem.ConstructYr, "height": elem.Height, "area": elem.Area}
			items = append(items, building)
		}
		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
		cur.Close(context.TODO())
		json.NewEncoder(w).Encode(items)
	}
	return http.HandlerFunc(fn)
}

//GetBuildingByID ... Get a building by its ID (BIN) param
func GetBuildingByID(c *mongo.Collection) http.HandlerFunc {
	var result Building
	fn := func(w http.ResponseWriter, r *http.Request) {
		idStr := r.URL.Query().Get("id")
		id, err := strconv.ParseFloat(idStr, 64)
		error := c.FindOne(context.TODO(), bson.M{"id": id}).Decode(&result)
		if error != nil {
			log.Fatal(err)
		}
		building := bson.M{"id": result.ID, "constructyr": result.ConstructYr, "height": result.Height, "area": result.Area}
		json.NewEncoder(w).Encode(building)
	}
	return http.HandlerFunc(fn)
}

//GetByConstructYr ... Get list of buildings by year param
func GetByConstructYr(c *mongo.Collection) http.HandlerFunc {
	items := bson.A{}
	fn := func(w http.ResponseWriter, r *http.Request) {
		yrStr := r.URL.Query().Get("year")
		yr, err := strconv.ParseFloat(yrStr, 32)
		cur, err := c.Find(context.TODO(), bson.M{"constructyr": yr})
		if err != nil {
			log.Fatal(err)
		}
		for cur.Next(context.TODO()) {
			var elem Building
			err := cur.Decode(&elem)
			if err != nil {
				log.Fatal(err)
			}
			building := bson.M{"id": elem.ID, "constructyr": elem.ConstructYr, "height": elem.Height, "area": elem.Area}
			items = append(items, building)
		}
		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
		cur.Close(context.TODO())
		json.NewEncoder(w).Encode(items)
	}
	return http.HandlerFunc(fn)
}

//GetLessThanHeight ... Get all buildings smaller than a certain height, sorted by decreasing height
func GetLessThanHeight(c *mongo.Collection) http.HandlerFunc {
	response := bson.A{}
	fn := func(w http.ResponseWriter, r *http.Request) {
		heightStr := r.URL.Query().Get("height")
		height, err := strconv.ParseFloat(heightStr, 64)
		group := []bson.M{{"$match": bson.M{"height": bson.M{"$lt": height}}}, {"$sort": bson.M{"height": -1}}}
		cur, err := c.Aggregate(
			context.TODO(),
			group,
		)
		if err != nil {
			fmt.Println(err)
		}
		for cur.Next(context.TODO()) {
			var elem Building
			err := cur.Decode(&elem)
			if err != nil {
				log.Fatal(err)
			}
			building := bson.M{"id": elem.ID, "constructyr": elem.ConstructYr, "height": elem.Height, "area": elem.Area}
			response = append(response, building)
		}
		cur.Close(context.TODO())
		json.NewEncoder(w).Encode(response)
		response = nil
	}
	return http.HandlerFunc(fn)
}

//CalculateData ... Caculate total area, average height, and building count
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
			sum += elem.Area
			heightSum += elem.Height
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

//GroupByYear ... Aggregate and count building ID's by year
func GroupByYear(c *mongo.Collection) http.HandlerFunc {
	response := bson.A{}
	fn := func(w http.ResponseWriter, r *http.Request) {
		years, err := c.Distinct(context.TODO(), "constructyr", bson.D{})
		if err != nil {
			fmt.Println(err)
		}
		for _, year := range years {
			group := []bson.M{{"$match": bson.M{"constructyr": year}}}
			cur, err := c.Aggregate(
				context.TODO(),
				group,
			)
			if err != nil {
				fmt.Println(err)
			}
			var items []int64
			for cur.Next(context.TODO()) {
				var elem Building
				err := cur.Decode(&elem)
				if err != nil {
					log.Fatal(err)
				}
				items = append(items, elem.ID)
			}
			cur.Close(context.TODO())
			response = append(response, bson.M{"constructyr": year, "count": len(items), "buildings": items})
		}
		json.NewEncoder(w).Encode(response)
		response = nil
	}
	return http.HandlerFunc(fn)
}
