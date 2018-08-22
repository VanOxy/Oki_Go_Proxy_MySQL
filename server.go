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
const (
	MYSQL   = "192.168.1.115:3306" // MaxScale
	PROXY   = "192.168.1.100:3306" // THIS SERVER
	DB_NAME = "pure"
)

func main() {
	handler.SetDbName(DB_NAME)
	handler.SetInitState(true)

	// Create history database if not exist
	//Initialisation()

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

	tables := GetTablesFromRelationalDatabase()
	fmt.Println(tables)
	os.Exit(3)
	db_dump := GetDbStructure(tables)
	CreateHistoryDb(db_dump)

	// *** CREATE DB & TABLES INTO ColumnStore ***
	db_mcs, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.121)/")
	if err != nil {
		panic(err.Error())
	}
	defer db_mcs.Close()

	_, err = db_mcs.Exec("CREATE DATABASE IF NOT EXISTS " + DB_NAME)
	if err != nil {
		panic(err)
	}

	_, err = db_mcs.Exec("USE okidb")
	if err != nil {
		panic(err)
	}

	// create tables
	for i := range tables {
		query := "CREATE TABLE IF NOT EXISTS " + tables[i] + " (id INT, column_name VARCHAR(30), value VARCHAR(50), timestamp DATETIME) engine=columnstore"
		// id INT NOT NULL COMMENT 'autoincrement=1',
		_, err = db_mcs.Exec(query)
		if err != nil {
			panic(err)
		}
	}

	handler.SetInitState(true)
}

// *** RETURNS TABLES FROM RELATIONAL DB ***
func GetTablesFromRelationalDatabase() []string {
	// connect to maxScale
	db_mxsc, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/"+DB_NAME)
	if err != nil {
		panic(err.Error())
	}
	defer db_mxsc.Close()

	// query
	res, err := db_mxsc.Query("SHOW TABLES FROM " + DB_NAME)
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

	// tables
	var tables []string

	// Fetch res
	for res.Next() {
		// get RawBytes from data
		err = res.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}
		// Now do something with the data.
		// Here we just print each column as a string.
		var value string
		for i := range values {
			// Here we can check if the value is nil (NULL value)
			if values[i] == nil {
				value = "NULL"
			} else {
				value = string(values[i])
				tables = append(tables, value)
			}
		}
	}
	if err = res.Err(); err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	return tables
}

func GetDbStructure(tables []string) [][]string {

	structure := make([][]string, len(tables))

	db_mxsc, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/"+DB_NAME)
	if err != nil {
		panic(err.Error())
	}
	defer db_mxsc.Close()

	for _, tableName := range tables {

		// query
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

		// Fetch res
		for res.Next() {
			// get RawBytes from data
			err = res.Scan(scanArgs...)
			if err != nil {
				panic(err.Error())
			}
			// Now do something with the data.
			// Here we just print each column as a string.
			var value string
			for i := range values {
				// Here we can check if the value is nil (NULL value)
				if values[i] == nil {
					value = "NULL"
				} else {
					value = string(values[i])
					tables = append(tables, value)
				}
			}
		}

		if err = res.Err(); err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

	}

	return structure
}

func CreateHistoryDb(db_dump [][]string) {

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
