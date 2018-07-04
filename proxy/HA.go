package proxy

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "../go-sql-driver/mysql"
)

// database name in columnStore
var dbName string = "pure"

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

	// **************** get id of inserted item ****************
	// **********************************************************
	// connect relative DB to get the last id inserted
	db, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/"+dbName) // you can precise port adress:3307 for Maxscale using columnStore cluster
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
	db_mcs, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.121)/"+dbName)
	if err != nil {
		panic(err.Error())
	}
	defer db_mcs.Close()

	// executer les requette
	for i := range queryArguments {
		//fmt.Println(queryArguments[i])
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

	// get indexes
	initIndex := len("UPDATE")
	setIndex := strings.Index(okQuery, "SET")
	whereIndex := strings.Index(okQuery, "WHERE")

	// get table name
	tableName := strings.TrimSpace(okQuery[initIndex:setIndex])

	// get column and its value
	valueParams := strings.Split(strings.TrimSpace(okQuery[setIndex+3:whereIndex]), "=")
	for i := range valueParams {
		valueParams[i] = strings.Trim(valueParams[i], " '")
	}
	column := valueParams[0]
	value := valueParams[1]

	// get id
	idParams := strings.Split(strings.TrimSpace(okQuery[whereIndex+5:len(okQuery)]), "=")
	for i := range idParams {
		idParams[i] = strings.Trim(idParams[i], " ")
	}
	itemId := idParams[1]

	// build query params
	queryArguments := []interface{}{itemId, column, value, time.Now().Format("2006-01-02 15:04:05")}

	// connect HA
	db_mcs, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.121)/"+dbName)
	if err != nil {
		panic(err.Error())
	}
	defer db_mcs.Close()

	// executer la requette
	_, errex := db_mcs.Exec("INSERT INTO "+tableName+" VALUES (?, ?, ?, ?)", queryArguments...)
	if errex != nil {
		panic(errex.Error())
	}
}

// v2
func PerformSelectQuery(query string) {
	// sql = "SELECT * FROM articles WHERE id=40 HISTORY t2"
	// sql = "SELECT * FROM articles WHERE id=45 HISTORY BETWEEN t1, t2"

	// get query slice
	okQuery := strings.TrimSpace(query)

	// get indexes
	//initIndex := len("SELECT")
	fromIndex := strings.Index(okQuery, "FROM")
	whereIndex := strings.Index(okQuery, "WHERE")
	historyIndex := strings.Index(okQuery, "HISTORY")

	// get tablename
	tableName := strings.TrimSpace(okQuery[fromIndex+4 : whereIndex])

	// get id
	idParams := strings.Split(strings.TrimSpace(okQuery[whereIndex+5:historyIndex]), "=")
	itemId := strings.Trim(idParams[1], " ")

	// get columns
	//columns := strings.Trim(okQuery[6:fromIndex], " ")
	//columns = columns + ", timestamp"

	/*
		// get select value(s)
		var selectParams []string
		if strings.Contains(okQuery[initIndex:fromIndex], "*") {
			selectParams[0] = "*"
		} else {
			selectParams = strings.Split(strings.TrimSpace(okQuery[initIndex:fromIndex]), ",")
			for i := range selectParams {
				selectParams[i] = strings.Trim(selectParams[i], " '")
			}
		}
	*/

	// connect HA
	db_mcs, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.121)/"+dbName)
	if err != nil {
		panic(err.Error())
	}
	defer db_mcs.Close()

	// get type --> between or not?
	if strings.Contains(okQuery[historyIndex+7:len(okQuery)], "BETWEEN") {
		// sql = "SELECT * FROM MyGuests WHERE id=45 HISTORY BETWEEN t1, t2"

		// get dates
		dates := strings.Split(strings.TrimSpace(okQuery[historyIndex+15:len(okQuery)]), ",")
		for i := range dates {
			dates[i] = strings.Trim(dates[i], " ")
			fmt.Println("colunms:", dates[i])
		}

		query := okQuery[:historyIndex] + "AND '" + dates[0] + "' <= timestamp AND timestamp <= '" + dates[1] + "' ORDER BY timestamp"

		// see the stackTrace of function call
		// insert channel into functions untill get into readPacket()

		// we don't analyse the query result because we sniff packets
		_, err := db_mcs.Query(query)
		if err != nil {
			panic(err)
		}

	} else {
		// sql = "SELECT * FROM MyGuests WHERE id=45 HISTORY 2009-10-20"

		var date string = strings.TrimSpace(okQuery[historyIndex+7 : len(okQuery)])
		query := okQuery[:fromIndex-1] + ", timestamp " + okQuery[fromIndex:historyIndex] + "AND timestamp IN (SELECT MAX(timestamp) FROM " + tableName + " WHERE id = " + itemId + " AND timestamp < '" + date + "')"

		// see the stackTrace of function call
		// insert channel into functions untill get into readPacket()

		_, err := db_mcs.Query(query)
		if err != nil {
			panic(err)
		}
	}
}

/*
// v1
func PerformSelectQuery(query string) {
	// sql = "SELECT * FROM MyGuests WHERE id=45 HISTORY t2"
	// sql = "SELECT * FROM MyGuests WHERE id=45 HISTORY BETWEEN t1, t2"

	// get query slice
	okQuery := strings.TrimSpace(query)

	// get indexes
	//initIndex := len("SELECT")
	fromIndex := strings.Index(okQuery, "FROM")
	whereIndex := strings.Index(okQuery, "WHERE")
	historyIndex := strings.Index(okQuery, "HISTORY")

	// get tablename
	tableName := strings.TrimSpace(okQuery[fromIndex+4 : whereIndex])

	// get id
	idParams := strings.Split(strings.TrimSpace(okQuery[whereIndex+5:historyIndex]), "=")
	for i := range idParams {
		idParams[i] = strings.Trim(idParams[i], " ")
	}
	itemId := idParams[1]

	// ----------------------- to comment --------------------------------
		// get select value(s)
		var selectParams []string
		if strings.Contains(okQuery[initIndex:fromIndex], "*") {
			selectParams[0] = "*"
		} else {
			selectParams = strings.Split(strings.TrimSpace(okQuery[initIndex:fromIndex]), ",")
			for i := range selectParams {
				selectParams[i] = strings.Trim(selectParams[i], " '")
			}
		}
	// -----------------------------------------------------------------------

	// connect HA
	db_mcs, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.121)/okidb")
	if err != nil {
		panic(err.Error())
	}
	defer db_mcs.Close()

	// get type --> between or not?
	if strings.Contains(okQuery[historyIndex+7:len(okQuery)], "BETWEEN") {
		// sql = "SELECT * FROM MyGuests WHERE id=45 HISTORY BETWEEN t1, t2"

		// get dates
		dates := strings.Split(strings.TrimSpace(okQuery[historyIndex+15:len(okQuery)]), ",")
		for i := range dates {
			dates[i] = strings.Trim(dates[i], " ")
			//fmt.Println("colunms:", columns[i])
		}

		query := "SELECT column_name as 'column', value, timestamp FROM persons WHERE " + dates[0] + " < timestamp AND timestamp < " + dates[1] + " ORDER BY timestamp"

		// see the stackTrace of function call
		// insert channel into functions untill get into readPacket()

		// we don't analyse the query result because we sniff packets
		_, err := db_mcs.Query(query)
		if err != nil {
			panic(err)
		}

	} else {
		// sql = "SELECT * FROM MyGuests WHERE id=45 HISTORY 2009-10-20"

		var time string = strings.TrimSpace(okQuery[historyIndex+7 : len(okQuery)])
		query := "SELECT column_name as 'column', value, timestamp FROM " + tableName + " WHERE timestamp IN (SELECT MIN(timestamp) FROM " + tableName + " WHERE id = " + itemId + " AND timestamp > timestamp('" + time + " 23:59:59') GROUP BY column_name) ORDER BY column_name ASC"

		// see the stackTrace of function call
		// insert channel into functions untill get into readPacket()

		_, err := db_mcs.Query(query)
		if err != nil {
			panic(err)
		}
	}
}
*/

func PerformDeleteQuery(query string) {
	// TODO
}
