package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"

	helper "./helper"
	dbms "./mysql"
)

const (
	MYSQL = "192.168.1.110:3306"
	PROXY = "192.168.1.100:3306"
)

func main() {

	testFunc()

	os.Exit(3)

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

	// implement method to get query

	// implement method to get 'mysql' object coming from real server

	//io.Copy(mysql, conn)
}

func appToMysql(app net.Conn, mysql net.Conn) {
	for {

		pkt, err := dbms.ProxyPacket(app, mysql)
		if err != nil {
			break
		}

		if query, err := dbms.GetQueryString(pkt); err == nil {
			fmt.Println("Query --> ", query)
		}
	}
}

func testFunc() {
	//query := "select * from persons where id=45 and value>125"
	query2 := 3
	//packet := helper.EncodeStringInBytes(query)
	packet2 := helper.EncodeIntInBytes(query2)
	fmt.Println(packet2)
}
