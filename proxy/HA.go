package proxy

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "../go-sql-driver/mysql"
)

func PerformInsertQuery(query string, channel chan struct{}) {

	// sql = "INSERT INTO persons (name, age) VALUES('type1', 15)"

	// get query slice
	okQuery := strings.TrimSpace(query)

	// get indexes
	initIndex := len("INSERT INTO")
	valIndex := strings.Index(okQuery, "VALUES")
	columnsIndex := strings.Index(okQuery, "(")

	// get table name
	tableName := strings.TrimSpace(okQuery[initIndex:columnsIndex])
	//fmt.Println("tableName:", tableName)

	// get columns
	columns := strings.Split(strings.TrimSpace(okQuery[columnsIndex:valIndex]), " ")
	for i := range columns {
		columns[i] = strings.Trim(columns[i], "(,)")
		//fmt.Println("colunms:", columns[i])
	}

	// get params
	params := strings.Split(strings.TrimSpace(okQuery[valIndex+6:len(okQuery)]), " ")
	for i := range params {
		params[i] = strings.Trim(params[i], "(,)'")
		//fmt.Println("params:", params[i])
	}

	// ********* get id of inserted item *********

	// connect relative DB to get the last id inserted
	db, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/okidb")
	if err != nil {
		panic(err.Error())
	}

	// declare id variable
	var itemId int

	// get id of inserted item
	row := db.QueryRow("SELECT MAX(id)+1 FROM " + tableName)
	switch err := row.Scan(&itemId); err {
	case sql.ErrNoRows:
		fmt.Println("No rows were returned!")
		break
	case nil:
		break
	default:
		panic(err)
	}
	// *****************************************************
	// *****************************************************

	// close channel to deblock initial thread, because now it can writte into main DB
	close(channel)
	// close db conection
	db.Close()

	// *********** Insert into HA ************
	queryArguments := make([][]interface{}, len(columns))
	for i := range columns {
		queryArguments[i] = []interface{}{itemId, columns[i], params[i], time.Now().Format("2006-01-02 15:04:05")}
	}

	// connect HA
	db_mcs, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.121)/okidb")
	if err != nil {
		panic(err.Error())
	}
	defer db_mcs.Close()

	// executer les requette
	for i := range queryArguments {
		fmt.Println(queryArguments[i])
		_, errex := db_mcs.Exec("INSERT INTO "+tableName+" VALUES (?, ?, ?, ?)", queryArguments[i]...)
		if errex != nil {
			panic(errex.Error())
		}
	}
	// ****************************************
}

func PerformUpdateQuery(query string) {

	// sql = "UPDATE MyGuests SET lastname='Doe' WHERE id=2"

	// get query slice
	okQuery := strings.TrimSpace(query)
	fmt.Println("\nEnter fct, query:", okQuery)

	// get indexes
	initIndex := len("UPDATE")
	setIndex := strings.Index(okQuery, "SET")
	whereIndex := strings.Index(okQuery, "WHERE")

	// get table name
	tableName := strings.TrimSpace(okQuery[initIndex:setIndex])
	fmt.Println("tablename:", tableName)

	// get columns
	valueParams := strings.Split(strings.TrimSpace(okQuery[setIndex+3:whereIndex]), "=")
	for i := range valueParams {
		valueParams[i] = strings.Trim(valueParams[i], " ")
		fmt.Println("valueParam:", valueParams[i])
	}

	// get id
	idParams := strings.Split(strings.TrimSpace(okQuery[whereIndex+5:len(okQuery)]), "=")
	for i := range idParams {
		idParams[i] = strings.Trim(idParams[i], " ")
		fmt.Println("----------")
		fmt.Println("idParam:", idParams[i])
		fmt.Println("----------")
	}
	//id := idParams[1]

	/*
		// final query
		//qr := "INSERT INTO " + tableName + " ("

		// connect HA

			db, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/okidb")
			if err != nil {
				panic(err.Error())
			}
			defer db.Close()

			// executer la requette
			_, errex := db.Exec(query)
			if errex != nil {
				panic(errex.Error())
			}
	*/

}
func PerformSelectQuery(query string) {

	// sql = "SELECT * FROM MyGuests WHERE id=45 HISTORY t2"
	// sql = "SELECT * FROM MyGuests WHERE id=45 HISTORY BETWEEN t1, t2"

	// get query slice
	okQuery := strings.TrimSpace(query)

	// get indexes
	initIndex := len("SELECT")
	fromIndex := strings.Index(okQuery, "FROM")
	whereIndex := strings.Index(okQuery, "WHERE")
	historyIndex := strings.Index(okQuery, "HISTORY")

	// get selects
	if strings.Contains(okQuery[initIndex:fromIndex], "*") {

	} else {

	}

	// get tablename
	tableName := strings.TrimSpace(okQuery[fromIndex+4 : whereIndex])
	fmt.Println("tablename:", tableName)

	// get history type
	if strings.Contains(okQuery[historyIndex+7:len(okQuery)], "BETWEEN") {

	} else {

	}
}

func PerformDeleteQuery(query string) {

}
