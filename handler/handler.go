package handler

import driver "../go-sql-driver/mysql"

func SetInitState(IsInitialisationOK bool) {
	driver.SetInitState(IsInitialisationOK)
}

func AllocateChannel(channel *chan []byte) {
	driver.AllocateChannel(channel)
}

func AllocateQuery(query string) {
	driver.AllocateQuery(query)
}

func ActivateSniffing() {
	driver.SetAllowSniffing(true)
}
