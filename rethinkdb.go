package dbquery

import (
	"fmt"

	rethinkdb "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

type rdbSess struct {
	session   *rethinkdb.Session
	dbName    string
	tableName string
}

type Order struct {
	Uid           string `json:"id" rethinkdb:"id,omitempty"`
	Orderid       string `json:"orderid" rethinkdb:"orderid,omitempty"`
	Name          string `json:"name" rethinkdb:"Name"`
	Address       string `json:"address" rethinkdb:"Address"`
	Time          string `json:"Time" rethinkdb:"Time"`
	Delivered     bool   `json:"delivered" rethinkdb:"Delivered"`
	Phone         string `json:"Phone" rethinkdb:"Phone"`
	Date          string `json:"Date" rethinkdb:"Date"`
	DriverID      string `json:"DriverID" rethinkdb:"DriverID"`
	DeliveredTime string `json:"DeliveredTime" rethinkdb:"DeliveredTime"`
}

func Connection(dbname, tablename string) rdbSess {
	var rdbSession rdbSess

	session, err := rethinkdb.Connect(rethinkdb.ConnectOpts{
		Address: "localhost:28015",
	})

	if err != nil {
		panic(err)
	}

	fmt.Println(session)

	rdbSession.session = session
	rdbSession.dbName = dbname
	rdbSession.tableName = tablename
	return rdbSession

}

func (r *rdbSess) Insert(data interface{}) error {
	_, err := rethinkdb.DB(r.dbName).Table(r.tableName).Insert(data).RunWrite(r.session)
	if err != nil {
		return err
	}

	return nil
}

func (r *rdbSess) GetAll() ([]Order, error) {
	var result []Order

	rows, err := rethinkdb.DB(r.dbName).Table(r.tableName).Run(r.session)
	if err != nil {
		return result, err
	}

	var orders []Order
	err = rows.All(&orders)
	if err != nil {
		return result, err
	}

	for _, p := range orders {
		result = append(result, p)
	}

	return result, nil
}

func (r *rdbSess) delete(id string) error {
	err := rethinkdb.DB(r.dbName).Table(r.tableName).Get(id).Delete().Exec(r.session)
	if err != nil {
		return err
	}

	return nil
}
