package main

import (
	"fmt"
	"os"
	"os/signal"

	"L0/db"
	"L0/stan"
	stan_stream "github.com/nats-io/stan.go"
)

const (
	ClusterName = "test-cluster"
	ClientName  = "MainClient"
	Channel     = "test"
)

const (
	User_name = "go_client"
	User_pass = "go_passwd"
	Db_name   = "wb_l0"
)

func main() {
	// Канал для пересылки данных из брокера в бд
	json_transfer := make(chan stan.MetaRoot, 64) // ? чётонадо переделать, надеятся на большой буффер будто бы плохая идея
	defer close(json_transfer)

	// Инициализация брокера
	conn, sc := natsSide(json_transfer)
	defer conn.Close()
	defer sc.Close()

	// Инициализация БД
	engine := db.NewEngine(User_name, User_pass, Db_name)
	defer engine.DB.Close()

	engine.CreateTables()

	// Компиляция запросов
	q := db.MakeQuery(*engine)
	defer q.Close()
	defer fmt.Println("[Info] Пака")

	// Получатель сообщений брокера - отслыает данные в бд
	go dbSender(q, json_transfer)

	// Завершение работы по прерыванию
	sigs := make(chan os.Signal, 1)
	done := make(chan any, 1)

	go func() {
		sig := <-sigs
		fmt.Printf("\n[Info] Signal caught - %v\n", sig)
		done <- true
	}()

	signal.Notify(sigs, os.Interrupt)

	<-done
}

// Инициализация и подписка на Nats топик
func natsSide(out chan stan.MetaRoot) (stan_stream.Conn, stan_stream.Subscription) {
	handler := stan.Handler{
		Callback: stanListener,
		Topic:    Channel,
		Out:      out,
	}
	conn := stan.StanConn(ClusterName, ClientName)

	sc, err := stan.Sub(conn, handler)
	if err != nil {
		war := fmt.Errorf("[Warning] Subscription to the channel %s failed due to: %w", Channel, err)
		fmt.Print(war, "\n\n")
	}
	return conn, sc
}

// Парсинг и валидация пришедших json ов. Шлёт результат в канал out
func stanListener(m *stan_stream.Msg, out chan<- stan.MetaRoot) {
	parsed, err := stan.Parse2Struct(m)
	if err != nil {
		err_msg := fmt.Errorf("[Warning] Nats msg parse error: %w", err)
		fmt.Print(err_msg, "\n\n")
		return
	}
	valid, err := stan.Validate(parsed)
	if err != nil {
		err_msg := fmt.Errorf("[Warning] Nats msg validation error: %w", err)
		fmt.Print(err_msg, "\n\n")
		return
	}
	fmt.Printf("[Info] Got msg: type - %T\n", valid)
	// stan.MsgPrinter(m)
	out <- valid
}

// Запись полученных сообщений в базу данных
func dbSender(q *db.Query, inp <-chan stan.MetaRoot) {
	for msg := range inp {
		id, err := q.SetOrder(&msg)

		if err != nil {
			fmt.Printf("[Warning] Error writing to the db: %s\n", err)
			return
		}

		fmt.Printf("[Info] Successful db entry. Msg id - %s\n", id)
	}
}
