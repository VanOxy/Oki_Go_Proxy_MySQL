package mysql

import (
	"errors"
	"io"
	"net"
)

const (
	COM_QUERY        = 3
	COM_STMT_PREPARE = 22
)

// Determiner les erreurs possibles
var ErrWritePacket = errors.New("error while writing packet payload")
var ErrNoQueryPacket = errors.New("malformed packet")

// prends en parametre les connections de client et du serveur MySQL
func ProxyPacket(src, dst net.Conn) ([]byte, error) {
	pkt, err := ReadPacket(src)
	if err != nil {
		return nil, err
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

	bodyLength := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

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
