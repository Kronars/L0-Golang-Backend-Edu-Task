package db

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func NewEngine(User_name, User_pass, Db_name string) *sql.DB {
	сonnStr := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable options='--client_encoding=UTF8'", User_name, User_pass, Db_name)

	db, err := sql.Open("postgres", сonnStr)
	if err != nil {
		panic(err)
	}
	return db
}
