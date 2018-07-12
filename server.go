package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"

	handler "./handler"
	dbms "./proxy"
)

/*
SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
*/
const (
	MYSQL = "192.168.1.115:3306" // MaxScale
	PROXY = "192.168.1.100:3306" // THIS SERVER
)

func main() {
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

	fmt.Println("Proxy_server connected to MySQL_server")

	// ***** INIT CONNECION *****
	// copy traffic from 'mysql' to 'conn' -> client
	// -> because of mysqlProto which require mysql_server sends 'Greeting' packet first, after what clients responds with hashed login/passwd
	//go io.Copy(conn, mysql)
	go MysqlToApp(mysql, conn)

	// copy traffic form conn_client to mysql_server
	appToMysql(conn, mysql)

}

func appToMysql(client net.Conn, mysql net.Conn) {
	for {
		err := dbms.ProxyPacket(client, mysql, "mysql")
		if err != nil {
			fmt.Println("mysql : " + err.Error())
			break
		}
	}
}

func MysqlToApp(mysql net.Conn, client net.Conn) {
	for {
		err := dbms.ProxyPacket(mysql, client, "client")
		if err != nil {
			fmt.Println("client : " + err.Error())
			break
		}
	}
}

// create new db if not exist as well, as tables in fucntion of RDBMS tables
func Initialisation() {

	handler.SetInitState(false)

	// *** GET TABLES FROM RELATIONAL ***
	// connect to maxScale
	db_mxsc, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/okidb")
	if err != nil {
		panic(err.Error())
	}
	defer db_mxsc.Close()

	// query
	rows, err := db_mxsc.Query("SHOW TABLES FROM okidb")
	if err != nil {
		panic(err.Error())
	}

	// Get column names
	columns, err := rows.Columns()
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

	// Fetch rows
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
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
	if err = rows.Err(); err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	// *** CREATE DB & TABLES INTO ColumnStore ***
	db_mcs, err := sql.Open("mysql", "okulich:22048o@tcp(192.168.1.121)/")
	if err != nil {
		panic(err.Error())
	}
	defer db_mcs.Close()

	_, err = db_mcs.Exec("CREATE DATABASE IF NOT EXISTS okidb")
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

	tables = nil
	handler.SetInitState(true)
}
