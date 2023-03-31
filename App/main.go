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
	// Обработчик сообщений из Nats
	conn, sc := NatsSide()
	defer conn.Close()
	defer sc.Close()

	// Инициализация БД
	engine := db.NewEngine(User_name, User_pass, Db_name)
	engine.CreateTables()

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
	fmt.Println("[Info] Пака")
}

func NatsSide() (stan_stream.Conn, stan_stream.Subscription) {
	callback := stan.Handler(StanListener)

	// Инициализация и подписка на Nats топик
	conn := stan.StanConn(ClusterName, ClientName)

	sc, err := stan.Sub(conn, Channel, callback)
	if err != nil {
		war := fmt.Errorf("[Warning] Subscription to the channel %s failed due to: %w", Channel, err)
		fmt.Print(war, "\n\n")
	}

	return conn, sc
}

func MsgPrinter(m *stan_stream.Msg) {
	msg := string(m.Data)
	if len(msg) > 10 {
		repl := msg[:20]
		repl += "... ..."
		repl += msg[len(msg)-20:]
		fmt.Printf("[Info] Recive msg: %s\n", repl)
	} else {
		fmt.Printf("[Info] Recive msg: %s\n", msg)
	}
}

func StanListener(m *stan_stream.Msg) {
	// MsgPrinter(m)
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
	fmt.Printf("[Info] Got msg: type - %T, msg - %#v\n\n", valid, valid)
}
