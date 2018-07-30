package handler

import driver "../go-sql-driver/mysql"

var dbName string

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

func SetDbName(name string) {
	dbName = name
}

func GetDbName() string {
	return dbName
}
