# Golang Buildings API
By Wahhaj Zahedi

Instructions to run:
  1. Clone the repo into the Go Path
  2. Ensure MongoDB Driver for Golang is installed and functional by following: 
  https://www.mongodb.com/blog/post/mongodb-go-driver-tutorial
    
  3. Run ```go run main.go``` in the project directory to run

API Documentation:
  * The http server runs on localhost port 80.
  * The following API endpoints are used:<br />
    * Home: http://localhost/<br />
    * Get all buildings: http://localhost/all <br />
    * Get building by ID(BIN): http://localhost/building?id= <br />
      - Example: http://localhost/building?id=3394646 <br />
    * Get sorted list of buildings with height less than x: http://localhost/smallerthan?height= <br />
      - Example: http://localhost/smallerthan?height=50 <br />
    * Get list of buildings by year: http://localhost/byyear?year= <br />
      - Example: http://localhost/byyear?year=1992 <br />
    * Caculate total area, average height, and building count: http://localhost/data <br />
    * Aggregate and count building ID's by year: http://localhost/groupyear <br />
    
    
