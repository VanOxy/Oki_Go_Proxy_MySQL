package mysql

import (
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	// client
	OK_Packet	= 0
	COM_QUIT  	= 1	
	COM_INIT_DB = 2
	COM_QUERY 	= 3
	// server
	OK_Pack 	= 254	
	ERR_Packet 	= 255
)

var counter = 1

// Determiner les erreurs possibles
var ErrWritePacket = errors.New("error while writing packet payload")
var ErrNoQueryPacket = errors.New("malformed packet")

// prends en parametre les connections de client et du serveur MySQL
func ProxyPacket(src, dst net.Conn) error {
	pkt, err := ReadPacket(src)
	if err != nil {
		return err
	}

	addr := src.RemoteAddr().String()
	if addr == "192.168.1.115:3306" { 		// mysql server
		fmt.Println(addr, " mysql: ", counter)
		fmt.Println(string(pkt))
		fmt.Println(pkt)
		counter++
	} else { 								// client
		fmt.Println(addr, " client: ", counter)
		fmt.Println(string(pkt))
		fmt.Println(pkt)
		counter++
	}
	fmt.Println("------------------------------------------------------------------------------------")

	/*
	// check if packet is querry
	if query, err := GetQueryString(pkt); err == nil {
		// lauch scan
		// if normal --> manage
		// *	*	*	*	get information if select/insert/update/delete
		// *	*	*	*	if select do nothing
		// *	*	*	*	if insert/update/delete --> see crem
		// if custom --> manage
		// *	*	*	*	only select here !!!
		// *	*	*	*	get info after ***
		// *	*	*	*	create second part of querry
		// *	*	*	*	create first part of querry, and append()
		// *	*	*	*	close connection to mysql
		// *	*	*	*	open connection to columnStore
		// *	*	*	*	get data & transmit them
		// *	*	*	*	return !
		fmt.Println("Query --> ", query)
	}
	*/

	_, err = WritePacket(pkt, dst)
	if err != nil {
		return err
	}

	return nil
}

// ReadPacket читает данные из conn, возвращая готовый пакет
func ReadPacket(conn net.Conn) ([]byte, error) {
	header := []byte{0, 0, 0, 0}

	if _, err := io.ReadFull(conn, header); err == io.EOF {
		return nil, io.ErrUnexpectedEOF
	} else if err != nil {
		return nil, err
	}

	bodyLength := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	// 00000000 00000000 00000000 00101011 --> h[0]
	// 00000000 00000000 01101000 00000000 --> h[1]<<8
	// 00000000 00101111 00000000 00000000 --> h[2]<<16
	// 00000000 00101111 01101000 00101011 --> bodyLength

	body := make([]byte, bodyLength)

	n, err := io.ReadFull(conn, body)
	if err == io.EOF {
		return nil, io.ErrUnexpectedEOF
	} else if err != nil {
		return nil, err
	}

	return append(header, body[0:n]...), nil
}

// WritePacket пишет пакет, полученный из метода ReadPacket, в conn
func WritePacket(pkt []byte, conn net.Conn) (int, error) {
	n, err := conn.Write(pkt)
	if err != nil {
		return 0, ErrWritePacket
	}

	return n, nil
}

// CanGetQueryString проверяет, является ли пакет командой COM_QUERY
func CanGetQueryString(pkt []byte) bool {
	return len(pkt) > 5 && (pkt[4] == COM_QUERY)
}

// returns packet value from 6th bite --> Query as String --> 1-4:header, 5:command, 6-n;querry
func GetQueryString(pkt []byte) (string, error) {
	if CanGetQueryString(pkt) {
		return string(pkt[5:]), nil
	}

	return "", ErrNoQueryPacket
}

func Clean() {
	counter = 1
}
