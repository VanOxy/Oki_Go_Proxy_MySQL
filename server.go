package main

import (
	"fmt"
	"io"
	"log"
	"net"

	helper "./helper"
	dbms "./mysql"
)

const (
	MYSQL = "192.168.1.55:3306"
	PROXY = "192.168.1.48:3306" // THIS SERVER
)

func main() {

	proxyListener, err := net.Listen("tcp", PROXY)
	if err != nil {
		log.Fatalf("%s: %s", "ERROR", err.Error())
	}
	defer proxyListener.Close()

	fmt.Println("Proxy started at : ", PROXY)

	for {
		conn, err := proxyListener.Accept()
		if err != nil {
			log.Printf("%s: %s", "ERROR", err.Error())
		}

		fmt.Println("connection accepted...")

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
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
	go io.Copy(conn, mysql)

	// copy traffic form conn_client to mysql_server
	appToMysql(conn, mysql)
}

func appToMysql(client net.Conn, mysql net.Conn) {
	for {
		_, err := dbms.ProxyPacket(client, mysql)
		if err != nil {
			dbms.Clean()
			break
		}
	}
}

func testFunc() {
	query := "DROP DATABASE okidb"
	packet := helper.StringToBytes(query)
	fmt.Println(packet)
}
