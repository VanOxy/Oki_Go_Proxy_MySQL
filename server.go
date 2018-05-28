package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"

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
	Initialisation()
	fmt.Println("Before exit")
	os.Exit(1)

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
	// fermer la connexion à la fin
	defer conn.Close()

	// connect MySQL server
	mysql, err := net.Dial("tcp", MYSQL)
	if err != nil {
		log.Fatalf("%s: %s", "ERROR", err.Error())
		return
	}

	fmt.Println("Proxy connected to MySQL")

	// ***** INIT CONNECION *****
	// copy traffic from 'mysql' to 'conn' -> client
	// -> because of mysqlProto which needs mysql sends 'Greeting' packet first, after what clients responds with login/passwd hashed
	//go io.Copy(conn, mysql)
	go MysqlToApp(mysql, conn)

	// copy traffic form conn_client to mysql_server
	appToMysql(conn, mysql)

	// do smth like
	/*
		if(appToMysql && MysqlToApp){
			continue
		}
	*/
}

func appToMysql(client net.Conn, mysql net.Conn) {
	for {
		err := dbms.ProxyPacket(client, mysql)
		if err != nil {
			break
		}
	}
	// send signal --> proxying done
}

func MysqlToApp(mysql net.Conn, client net.Conn) {
	for {
		err := dbms.ProxyPacket(mysql, client)
		if err != nil {
			break
		}
	}
}

// create new db if not exist as well, as tables in fucntion of RDBMS tables
func Initialisation() {
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

	fmt.Println("Show tables:")
	for i := range tables {
		fmt.Println(tables[i])
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
		query := "CREATE TABLE IF NOT EXISTS " + tables[i] + " (id INT NOT NULL COMMENT 'autoincrement=1', column_name VARCHAR(30) NOT NULL, value VARCHAR(50) NOT NULL, reg_date DATETIME NOT NULL) engine=columnstore"

		_, err = db_mcs.Exec(query)
		if err != nil {
			panic(err)
		}
	}

	tables = nil
}
