package main

import (
	"fmt"
	"log"
	"net"

	helper "./helper"
	dbms "./mysql"
)
/*
SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
*/
const (
	MYSQL = "192.168.1.115:3306"	// MaxScale
	PROXY = "192.168.1.100:3306" // THIS SERVER
)

func main() {

	listener, err := net.Listen("tcp", PROXY)
	if err != nil {
		log.Fatalf("%s: %s", "ERROR", err.Error())
	}
	defer listener.Close()

	fmt.Println("Proxy started at : ", PROXY)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("%s: %s", "ERROR", err.Error())
		}

		fmt.Println("connection accepted...")

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
			dbms.Clean()
			break
		}
	}
	// send signal --> proxying done
}

func MysqlToApp(mysql net.Conn, client net.Conn) {
	for {
		err := dbms.ProxyPacket(mysql, client)
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
