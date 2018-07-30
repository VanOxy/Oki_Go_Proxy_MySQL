package proxy

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "../go-sql-driver/mysql"
	handler "../handler"
)

// database name in columnStore
//var dbName string = "okidb"
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
	// *********************************************************

	// connect relative DB to get the last id inserted
	db, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/"+handler.GetDbName()) // you can precise port adress:3307 for Maxscale using columnStore cluster
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
		panic(err.Error())
	}

	// close channel to deblock initial thread, because now it can writte into main DB
	close(channel)
	// close db conection
	db.Close()

	// ***************************************
	// ***************************************

	// *********** Insert into HA ************
	// ***************************************

	// create query
	arguments := []interface{}{itemId}
	values := "(?,"
	for i := 0; i < len(params); i++ {
		arguments = append(arguments, params[i])

		if i == len(params)-1 {
			values = values + " ?, ?)"
		} else {
			values = values + " ?,"
		}
	}
	arguments = append(arguments, time.Now().Format("2006-01-02 15:04:05"))

	// connect HA
	db_mcs, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.121)/"+handler.GetDbName())
	if err != nil {
		panic(err.Error())
	}
	defer db_mcs.Close()

	qr := "INSERT INTO " + tableName + " VALUES " + values

	/*
		fmt.Println(qr)
		fmt.Println(arguments)
		os.Exit(3)
	*/

	// executer la requette
	_, errex := db_mcs.Exec(qr, arguments...)
	if errex != nil {
		panic(errex.Error())
	}
	// ***************************************
	// ***************************************
}

func PerformUpdateQuery(query string) {

	// sql = "UPDATE MyGuests SET lastname='Doe' WHERE id=2"

	// ********************* GET PARAMETERS **************************
	// ***************************************************************

	// get query slice
	okQuery := strings.TrimSpace(query)

	// get indexes
	initIndex := len("UPDATE")
	setIndex := strings.Index(okQuery, "SET")
	whereIndex := strings.Index(okQuery, "WHERE")

	// get table name
	tableName := strings.TrimSpace(okQuery[initIndex:setIndex])

	// get column and its value to UPDATE
	valuecolumns := strings.Split(strings.TrimSpace(okQuery[setIndex+3:whereIndex]), "=")
	for i := range valuecolumns {
		valuecolumns[i] = strings.Trim(valuecolumns[i], " '")
	}
	column := valuecolumns[0]
	value := valuecolumns[1]

	fmt.Println(column)
	fmt.Println(value)

	// get ID
	idcolumns := strings.Split(strings.TrimSpace(okQuery[whereIndex+5:len(okQuery)]), "=")
	for i := range idcolumns {
		idcolumns[i] = strings.Trim(idcolumns[i], " ")
	}
	itemId := idcolumns[1]

	// **************************************************
	// **************************************************

	// ***************** GET LAST DATA ******************
	// **************************************************

	db, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.121)/"+handler.GetDbName())
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	qr := "SELECT * FROM articles WHERE id=" + itemId + " ORDER BY timestamp DESC LIMIT 1"

	res, err := db.Query(qr)
	if err != nil {
		panic(err.Error())
	}

	// Get column names
	columns, err := res.Columns()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	// Make a slices for the values
	valuesArray := make([]string, len(columns))
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))

	// associate, because .Scan() needs an []interface{}
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Fetch rows
	for res.Next() {
		err = res.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}

		for i, val := range values {
			if val == nil {
				valuesArray[i] = "NULL"
			} else {
				valuesArray[i] = string(val)
			}
		}
	}

	if err = res.Err(); err != nil {
		panic(err.Error())
	}

	// *****************************************************
	// *****************************************************

	// ************ INSERT DATA ****************************
	// *****************************************************

	// build query columns
	arguments := []interface{}{} // in fact values
	valuesString := "(?,"

	for i := 0; i < len(columns); i++ {
		// manage data
		if column == columns[i] {
			arguments = append(arguments, value)
		} else if "timestamp" == columns[i] {
			arguments = append(arguments, time.Now().Format("2006-01-02 15:04:05"))
		} else {
			arguments = append(arguments, valuesArray[i])
		}

		// manage (?,?) values
		if i == len(columns)-2 {
			valuesString = valuesString + " ?)"
		} else if i == len(columns)-1 {
			// do nothing
		} else {
			valuesString = valuesString + " ?,"
		}
	}

	qr = "INSERT INTO " + tableName + " VALUES " + valuesString

	// executer la requette
	_, errex := db.Exec(qr, arguments...)
	if errex != nil {
		panic(errex.Error())
	}
	// *****************************************************
	// *****************************************************
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
		// -------------------------------------------------------------------
	*/

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

		handler.ActivateSniffing()
		executeQuery(query)

	} else {
		// sql = "SELECT * FROM MyGuests WHERE id=45 HISTORY 2009-10-20"

		var date string = strings.TrimSpace(okQuery[historyIndex+7 : len(okQuery)])
		query := okQuery[:fromIndex-1] + ", timestamp " + okQuery[fromIndex:historyIndex] + "AND timestamp IN (SELECT MAX(timestamp) FROM " + tableName + " WHERE id = " + itemId + " AND timestamp < '" + date + "')"

		handler.ActivateSniffing()
		executeQuery(query)
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
	db_mcs, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.121)/"+dbName)
	if err != nil {
		panic(err.Error())
	}
	defer db_mcs.Close()

	// get history type
	if strings.Contains(okQuery[historyIndex+7:len(okQuery)], "BETWEEN") {
		// sql = "SELECT * FROM MyGuests WHERE id=45 HISTORY BETWEEN t1, t2"

		// get dates
		dates := strings.Split(strings.TrimSpace(okQuery[historyIndex+15:len(okQuery)]), ",")
		for i := range dates {
			dates[i] = strings.Trim(dates[i], " ")
			//fmt.Println("colunms:", columns[i])
		}

		query := "SELECT column_name as 'column', value, timestamp FROM persons WHERE '" + dates[0] + "' < timestamp AND timestamp < '" + dates[1] + "' ORDER BY timestamp"

		// we don't analyse the query result because we sniff packets
		_, err := db_mcs.Query(query)
		if err != nil {
			panic(err)
		}

	} else {
		// sql = "SELECT * FROM MyGuests WHERE id=45 HISTORY 2009-10-20"

		var time string = strings.TrimSpace(okQuery[historyIndex+7 : len(okQuery)])
		query := "SELECT column_name as 'column', value, timestamp FROM " + tableName + " WHERE timestamp IN (SELECT MIN(timestamp) FROM " + tableName + " WHERE id = " + itemId + " AND timestamp > timestamp('" + time + " 23:59:59') GROUP BY column_name) ORDER BY column_name ASC"

		_, err := db_mcs.Query(query)
		if err != nil {
			panic(err)
		}
	}
}
*/

func PerformDeleteQuery(query string) {

}

func executeQuery(query string) {

	// connect HA
	db_mcs, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.121)/"+dbName)
	if err != nil {
		panic(err.Error())
	}
	defer db_mcs.Close()

	rows, err := db_mcs.Query(query)
	if err != nil {
		panic(err.Error())
	}

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Fetch rows
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		// Now do something with the data.
		// Here we just print each column as a string.
		//var value string
		for k, i := range values {
			_ = k
			_ = i
			// // Here we can check if the value is nil (NULL value)
			// if col == nil {
			// 	value = "NULL"
			// } else {
			// 	value = string(col)
			// }
			// fmt.Println(columns[i], ": ", value)
		}
	}

	if err = rows.Err(); err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
}
