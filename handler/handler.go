package handler

import (
	"database/sql"
	"time"

	driver "../go-sql-driver/mysql"
)

var dbName string
var MaxScaleConn *sql.DB
var ColumnStoreConn *sql.DB

func GetMaxScaleConn() (db **sql.DB) {
	return &MaxScaleConn
}

func SetMaxScaleConn() {
	var err error
	MaxScaleConn, err = sql.Open("mysql", "okulich:22048o@tcp(192.168.1.115)/"+GetDbName())
	if err != nil {
		panic(err.Error())
	}
	MaxScaleConn.SetConnMaxLifetime(time.Second * 2)
	MaxScaleConn.SetMaxIdleConns(0)
	MaxScaleConn.SetMaxOpenConns(5)
}

func GetColumnStoreConn() (db **sql.DB) {
	return &ColumnStoreConn
}

func SetColumnStoreConn() {
	var err error
	ColumnStoreConn, err = sql.Open("mysql", "okulich:22048o@tcp(192.168.1.121)/"+GetDbName())
	if err != nil {
		panic(err.Error())
	}
	ColumnStoreConn.SetConnMaxLifetime(time.Second * 30)
	ColumnStoreConn.SetMaxIdleConns(0)
	ColumnStoreConn.SetMaxOpenConns(5)
}

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
