package proxy

import (
	"encoding/binary"
	"fmt"
)

func GetPacket(pkt []byte) (string, error) {

	return string(pkt[5:]), nil
}

func StringToBytes(req string) (res []byte) {

	buff := []byte(req)

	return buff
}

func BytesToString(req []byte) (res string) {
	s := string(req)
	return s
}

func IntToBytes(req int) (res []byte) {

	var buff []byte

	n := binary.PutVarint(buff, int64(req))
	fmt.Println(n)

	return buff
}

func BytesToInt(req []byte) (res int) {

	return -1
}
