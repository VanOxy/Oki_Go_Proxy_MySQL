package proxy

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	handler "../handler"
)

const (
	OK_Packet   = 0
	COM_QUIT    = 1
	COM_INIT_DB = 2
	COM_QUERY   = 3
	EOF_Packet  = 254
	ERR_Packet  = 255
)

var mutex sync.Mutex

// Determiner les erreurs possibles
var ErrWritePacket = errors.New("error while writing packet payload")
var ErrNoQueryPacket = errors.New("malformed packet")
var ErrEndOfComm = errors.New("End of communication")

// prends en parametre les connections de client et du serveur MySQL
func ProxyPacket(src, dst net.Conn, mode string) error {

	var IsMutexLocked bool = false
	var IsQueryNormal bool = true

	pkt, err := ReadPacket(src)
	if err != nil {
		return err
	}

	if mode == "mysql" {
		fmt.Println("cli --> mysql:")
	} else {
		fmt.Println("mysql --> cli:")
	}

	// see packets
	printCommunication(pkt)

	// check if packet is querry
	if query, err := GetQueryString(pkt); err == nil {

		// create channel

		// get first 7 chars from query
		queryType := GetQueryType(query[0:7])

		switch queryType {
		case "select":
			// if classical query - nothing todo. If not:
			if strings.Contains(query, "HISTORY") {

				pktCh := make(chan []byte)
				handler.AllocateChannel(&pktCh)

				fmt.Println("SWITCH MODE..")

				IsQueryNormal = false // --> so interrupt comm & grap packets from HA
				go PerformSelectQuery(query)

				for packetFromHA := range pktCh {
					fmt.Println("pkt from HA : ", packetFromHA)
					_, err = WritePacket(packetFromHA, src)
				}
				return io.EOF

			}
			break
		case "insert":
			// mutex
			mutex.Lock()
			IsMutexLocked = true

			channel := make(chan struct{})
			timestamp := time.Now().Format("2006-01-02 15:04:05")

			go PerformInsertQuery(query, channel, timestamp)
			// waiting channel returns a value from thread 'PerformInsertQuery', kinda a sync or await_c#
			// closing channel is happening into 'PerformInsertQuery' method
			<-channel
			break
		case "update":
			go PerformUpdateQuery(query)
			break
		case "delete":
			// todo
			// copy query
			// go PerformDeleteQuery(query)
			// work query to send to HA Cluster
			// open conn & send
			break
		default:
			break
		}
	}

	// diff between SELECTs
	if IsQueryNormal {

		_, err = WritePacket(pkt, dst)
		if err != nil {
			return err
		}

		if IsMutexLocked {
			mutex.Unlock()
			IsMutexLocked = false
		}
	}

	return nil
}

// ReadPacket reads data form conn, returns a ready packet
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
	// 00000000 00101111 01101000 00101011 --> bodyLength (Int32)

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

func GetQueryType(query string) string {
	s := strings.ToLower(query)
	if strings.Contains(s, "select") {
		return "select"
	}
	if strings.Contains(s, "delete") {
		return "delete"
	}
	if strings.Contains(s, "insert") {
		return "insert"
	}
	if strings.Contains(s, "update") {
		return "update"
	}
	return ""
}

func printCommunication(pkt []byte) {
	fmt.Println(string(pkt))
	fmt.Println(pkt)
	fmt.Println("------------------------------------------------------------------------------------")
}
