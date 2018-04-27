package helper

func GetPacket(pkt []byte) (string, error) {

	return string(pkt[5:]), nil
}

func EncodeStringInBytes(req string) (res []byte) {

	buff := []byte(req)

	return buff
}

func EncodeIntInBytes(req int) (res byte) {

	buff := byte(req)

	return buff
}
