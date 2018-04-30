package mysql

import (
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	COM_QUIT         = 1
	COM_QUERY        = 3
	COM_STMT_PREPARE = 22
)

var counter = 1

// Determiner les erreurs possibles
var ErrWritePacket = errors.New("error while writing packet payload")
var ErrNoQueryPacket = errors.New("malformed packet")

// prends en parametre les connections de client et du serveur MySQL
func ProxyPacket(src, dst net.Conn) ([]byte, error) {
	pkt, err := ReadPacket(src)
	if err != nil {
		return nil, err
	}

	fmt.Println(counter, " : %x", pkt)
	counter++

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

	_, err = WritePacket(pkt, dst)
	if err != nil {
		return nil, err
	}

	return pkt, nil
}

// ReadPacket читает данные из conn, возвращая готовый пакет
func ReadPacket(conn net.Conn) ([]byte, error) {
	header := []byte{0, 0, 0, 0}

	if _, err := io.ReadFull(conn, header); err == io.EOF {
		return nil, io.ErrUnexpectedEOF
	} else if err != nil {
		return nil, err
	}

	fmt.Println(header[0], header[1], header[2], header[3])

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

// CanGetQueryString проверяет, является ли пакет командой COM_QUERY или COM_STMT_PREPARE
func CanGetQueryString(pkt []byte) bool {
	return len(pkt) > 5 && (pkt[4] == COM_QUERY || pkt[4] == COM_STMT_PREPARE)
}

// GetQueryString возвращает строку запроса, начиная с 6-го байта всего пакета
func GetQueryString(pkt []byte) (string, error) {
	if CanGetQueryString(pkt) {
		return string(pkt[5:]), nil
	}

	return "", ErrNoQueryPacket
}

func Clean() {
	counter = 1
}
