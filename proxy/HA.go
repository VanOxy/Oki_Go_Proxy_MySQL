package proxy

import (
	"database/sql"
	"fmt"
	"strings"

	_ "../go-sql-driver/mysql"
)

func PerformInsertQuery(query string) {

	// get query slice
	okQuery := strings.TrimSpace(query)
	fmt.Println("\nEnter fct, query:", okQuery)

	// get indexes
	initIndex := len("INSERT INTO")
	valIndex := strings.Index(okQuery, "VALUES")
	columnsIndex := strings.Index(okQuery, "(")

	// get table name
	tableName := strings.TrimSpace(okQuery[initIndex:columnsIndex])
	fmt.Println(tableName)

	// get columns
	columns := strings.Split(strings.TrimSpace(okQuery[columnsIndex:valIndex]), " ")
	for i := range columns {
		columns[i] = strings.Trim(columns[i], "(,)")
		fmt.Println(columns[i])
	}

	// get params
	params := strings.Split(strings.TrimSpace(okQuery[valIndex+6:len(okQuery)]), " ")
	for i := range params {
		params[i] = strings.Trim(params[i], "(,)")
		fmt.Println(params[i])
	}

	// connect main DB
	db, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/okidb")
	if err != nil {
		panic(err.Error())
	}
	//defer db.Close()

	// get id of inserted item
	var id int
	row := db.QueryRow("SELECT MAX(id)+1 FROM " + tableName)
	switch err := row.Scan(&id); err {
	case sql.ErrNoRows:
		fmt.Println("No rows were returned!")
		break
	case nil:
		break
	default:
		panic(err)
	}
	db.Close()
	fmt.Println(id)

	// final query
	//qr := "INSERT INTO " + tableName + " ("

	// connect HA
	/*
		db, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/okidb")
		if err != nil {
			panic(err.Error())
		}
		defer db.Close()
	*/

	/*
		// executer la requette
		_, errex := db.Exec(query)
		if errex != nil {
			panic(errex.Error())
		}
	*/
}

func PerformUpdateQuery(query string) {

	// start := time.Now()
}
func PerformDeleteQuery(query string) {

}
func PerformSelectQuery(query string) {
}
