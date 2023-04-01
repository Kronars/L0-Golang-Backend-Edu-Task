package db

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Engine struct {
	DB *sql.DB
}

func NewEngine(User_name, User_pass, Db_name string) *Engine {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable options='--client_encoding=UTF8'", User_name, User_pass, Db_name)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		err = fmt.Errorf("[Error] Failed to connect Postgres. connStr=%s Reason:\n%w", connStr, err)
		panic(err)
	}
	return &Engine{db}
}
