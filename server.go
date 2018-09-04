package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"

	_ "./go-sql-driver/mysql"
	handler "./handler"
	dbms "./proxy"
)

/*
	SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
*/

type Table struct {
	tableName string
	Params    [][]string
}

const (
	MYSQL       = "192.168.1.115:3306" // MaxScale
	PROXY       = "192.168.1.100:3306" // THIS SERVER
	ColumnStore = "192.168.1.121:3306"
	DB_NAME     = "pure"
)

func main() {

	Initialisation()

	// listen to port
	listener, err := net.Listen("tcp", PROXY)
	if err != nil {
		log.Fatalf("%s: %s", "ERROR", err.Error())
	}
	defer listener.Close()

	fmt.Println("Proxy started at : ", PROXY)

	// connection arrived
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("%s: %s", "ERROR", err.Error())
		}

		fmt.Println("connection accepted...")

		// launch in other thread
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	// fermer la connexion client à la fin
	defer conn.Close()

	// connect MySQL server
	mysql, err := net.Dial("tcp", MYSQL)
	if err != nil {
		log.Fatalf("%s: %s", "ERROR", err.Error())
		return
	}
	defer mysql.Close()

	fmt.Println("Proxy_server connected to MySQL_server")

	// ***** INIT CONNECION *****
	// copy traffic from 'mysql' to 'conn' -> client
	// -> because of mysqlProto which require mysql_server sends 'Greeting' packet first, after what clients responds with hashed login/passwd
	//go io.Copy(conn, mysql)
	go MysqlToApp(mysql, conn)

	// copy traffic form conn_client to mysql_server
	appToMysql(conn, mysql)

}

func appToMysql(client, mysql net.Conn) {
	for {
		err := dbms.ProxyPacket(client, mysql, "mysql")
		if err != nil {
			fmt.Println("mysql : " + err.Error())
			// may be close it here, or may be otherwise
			break
		}
	}
}

func MysqlToApp(mysql, client net.Conn) {
	for {
		err := dbms.ProxyPacket(mysql, client, "client")
		if err != nil {
			fmt.Println("client : " + err.Error())
			break
		}
	}
}

// create new res if not exist as well, as tables in fucntion of RDBMS tables
func Initialisation() {

	// to not sniff packets during initialisation
	handler.SetInitState(false)

	// set dbName
	handler.SetDbName(DB_NAME)

	tables := GetTablesFromRelationalDatabase()
	db_dump := GetTablesStructure(tables)
	CreateHistoryDb(db_dump)

	handler.SetInitState(true)
}

// *** RETURNS TABLES FROM RELATIONAL DB ***
func GetTablesFromRelationalDatabase() []string {
	// connect to MaxScale
	db_mxsc, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/")
	if err != nil {
		panic(err.Error())
	}
	defer db_mxsc.Close()

	// query
	res, err := db_mxsc.Query("show tables from " + handler.GetDbName())
	if err != nil {
		panic(err.Error())
	}

	columns, err := res.Columns()
	if err != nil {
		panic(err.Error())
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// tables
	var tables []string

	// Fetch res
	for res.Next() {
		err = res.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}
		var value string
		for i := range values {
			value = string(values[i])
			tables = append(tables, value)
		}
	}
	if err = res.Err(); err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	return tables
}

func GetTablesStructure(tables []string) []Table {

	var structures []Table

	db_mxsc, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/"+handler.GetDbName())
	if err != nil {
		panic(err.Error())
	}
	defer db_mxsc.Close()

	for _, tableName := range tables {

		// querr
		res, err := db_mxsc.Query("SHOW COLUMNS FROM " + tableName)
		if err != nil {
			panic(err.Error())
		}

		// Get column names
		columns, err := res.Columns()
		if err != nil {
			panic(err.Error())
		}
		// Make a slice for the values
		values := make([]sql.RawBytes, len(columns))
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		tableArguments := [][]string{}

		// Fetch res
		for res.Next() {
			err = res.Scan(scanArgs...)
			if err != nil {
				panic(err.Error())
			}

			var row []string
			for i := range values {
				if columns[i] == "Field" || columns[i] == "Type" || columns[i] == "Null" || columns[i] == "Default" {
					value := string(values[i])
					if value == "" {
						row = append(row, "null")
					} else {
						row = append(row, value)
					}
				}
			}
			tableArguments = append(tableArguments, row)
		}
		structures = append(structures, Table{tableName, tableArguments})

		if err = res.Err(); err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
	}
	return structures
}

// *** CREATE DB & TABLES INTO ColumnStore ***
func CreateHistoryDb(db_dump []Table) {
	db_mcs, err := sql.Open("mysql", "okulich:22048o@tcp("+ColumnStore+")/")
	if err != nil {
		panic(err.Error())
	}
	defer db_mcs.Close()

	_, err = db_mcs.Exec("CREATE DATABASE IF NOT EXISTS " + handler.GetDbName())
	if err != nil {
		panic(err)
	}

	_, err = db_mcs.Exec("USE " + handler.GetDbName())
	if err != nil {
		panic(err)
	}

	for _, table := range db_dump {

		tableName := table.tableName
		// create tables
		query := "CREATE TABLE IF NOT EXISTS " + tableName + " ("

		for _, tableParam := range table.Params { // récuperer la ligne
			for i, vals := range tableParam { // récuperer la colonne et ces attributs
				switch i {
				case 0:
					// column name
					query = query + vals
					break
				case 1:
					// main attributes
					query = query + " " + vals
					break
					/*
						case 2:
							// NULL
							if vals == "NO" {
								query = query + " NOT NULL"
							}
							if vals == "YES" {
								query = query + " NULL"
							}
							break
						case 3:
							// DEFAULT
							if vals != "null" {
								query = query + " DEFAULT '" + vals + "'"
							}
							break
					*/

				}
			}
			query = query + ", "
		}
		query = query + "timestamp DATETIME) engine=columnstore"
		// timestamp
		_, err = db_mcs.Exec(query)
		if err != nil {
			panic(err)
		}
	}
}

func testSomeFeatures() {

	//query := "SELECT column_name as 'column', value, timestamp FROM persons WHERE '2018-06-12 01:32:54' < timestamp AND timestamp < '2018-06-18 02:39:20'"
	// query := "INSERT INTO articles (id, ref, nom, stock, prix_vente, timestamp) SELECT (id, 'hkdfl', nom, stock, prix_vente, now()) FROM articles WHERE id=40"

	// **************************** wait input *********************************
	fmt.Print("insert y value here: ")
	input := bufio.NewScanner(os.Stdin)
	input.Scan()
	fmt.Println(input.Text())
	// *************************************************************************
}
