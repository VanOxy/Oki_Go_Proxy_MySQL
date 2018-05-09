package proxy

import (
	"database/sql"
	"fmt"
	"strings"

	_ "../go-sql-driver/mysql"
)

func PerformInsertQuery(query string) {

	okQuery := strings.TrimSpace(query)
	fmt.Println("\nEnter fct, query:", okQuery)

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

	db, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/okidb")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	/*
		err = db.Ping()
		if err != nil {
			panic(err.Error())
		}
	*/

	// executer la requette
	_, errex := db.Exec(query)
	if errex != nil {
		panic(errex.Error())
	}
}

func PerformUpdateQuery(query string) {

}
func PerformDeleteQuery(query string) {

}
func PerformSelectQuery(query string) {
}
