package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

/**
 * Func: InsertDataFromFile is for read the json file containing json array data
 *
 */
func InsertDataFromFile(db *sql.DB, filePath string, columnMapping map[string]string, table string) error {
	jsonData, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	return InsertData(db, jsonData, columnMapping, table)
}

/**
 * Func: joinStrings is for joining the string for raw query
 *
 * @author Mahendra Dwi Purwanto
 *
 */

func joinStrings(slice []string, sep string) string {
	if len(slice) == 0 {
		return ""
	}
	if len(slice) == 1 {
		return slice[0]
	}
	return slice[0] + sep + joinStrings(slice[1:], sep)
}

/**
 * Func: InsertData is for inserting the data into the database
 *
 * @author Mahendra Dwi Purwanto
 *
 */
func InsertData(db *sql.DB, jsonData []byte, columnMapping map[string]string, table string) error {
	// Init variable
	var dataSlice []map[string]interface{}
	var placeholders, columns []string
	var values []interface{}

	// Decode Json and set it to dataSlice
	err := json.Unmarshal(jsonData, &dataSlice)
	if err != nil {
		return err
	}

	// Build column and values for raw query
	for _, data := range dataSlice {
		var valueStrings []string
		for jsonKey, columnName := range columnMapping {
			if val, ok := data[jsonKey]; ok {
				values = append(values, val)
			} else {
				return fmt.Errorf("key %s not found in JSON data", jsonKey)
			}
			valueStrings = append(valueStrings, "?")
			columns = append(columns, columnName)
		}
		placeholders = append(placeholders, "("+joinStrings(valueStrings, ", ")+")")
	}

	// Set query
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, strings.Join(columns, ", "), joinStrings(placeholders, ", "))

	// Exec Query
	_, err = db.Exec(query, values...)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Setting up connection
	db, err := sql.Open("mysql", os.Getenv("DB_USERNAME")+":"+os.Getenv("DB_PASSWORD")+"@tcp("+os.Getenv("DB_HOST")+":"+os.Getenv("DB_PORT")+")/"+os.Getenv("DB_NAME"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Set the table name
	table := "tb_agama"

	// Get the file path
	filePath := "assets/example.json"

	// Mapping connection between key json to column name in table
	columnMapping := map[string]string{
		"id":         "id",
		"nama_agama": "name",
	}

	// Insert data handler
	err = InsertDataFromFile(db, filePath, columnMapping, table)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Data inserted successfully.")
}
