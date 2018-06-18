package handler

import driver "../go-sql-driver/mysql"

func SetInitState(IsInitialisationOK bool) {
	driver.SetInitState(IsInitialisationOK)
}
