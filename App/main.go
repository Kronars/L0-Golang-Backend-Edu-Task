package main

import (
	"encoding/json"
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
	// ------- Инициализация -------
	// Кеш - карта, индекс - order_uid
	cacheIndex := map[string]string{}

	// Инициализация БД
	engine := db.NewEngine(User_name, User_pass, Db_name)
	defer engine.DB.Close()
	// Создание таблиц. Старые не затирает
	engine.CreateTables()

	// Компиляция запросов
	q := db.MakeQuery(*engine)
	defer q.Close()
	defer fmt.Println("[Info] Пака")

	// ------- Конвеер --------
	// Последовательная выгрузка БД в кеш
	cacheLoad(*engine, &cacheIndex)

	// Инициализация брокера, подписки и обработчика: nats -> chan
	conn, sc, nats2db := newStanConn()
	defer sc.Close()
	defer conn.Close()

	// Обработчик: chan -> db
	// db2cache := sendFromNats2DB(q, nats2db)
	close(nats2db)

	// Кеширование новых записей
	// TODO: отсыл в кеш
	// cache2front := sendFromDB2cache(db2cache)

	// получение новых записей
	// TODO: отсылать по запросу на веб сервер

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

// Инициализация, подписка на Nats топик. Обработчик сообщений передаётся здесь
func newStanConn() (stan_stream.Conn, stan_stream.Subscription, chan stan.Message) {
	nats2db := make(chan stan.Message)
	handler := stan.Handler{
		Callback: natsReceiver,
		Topic:    Channel,
		Out:      nats2db,
	}
	conn := stan.StanConn(ClusterName, ClientName)

	sc, err := stan.Sub(conn, handler)
	if err != nil {
		war := fmt.Errorf("[Warning] Subscription to the channel %s failed due to: %w", Channel, err)
		fmt.Print(war, "\n\n")
	}
	return conn, sc, nats2db
}

// Парсинг и валидация пришедших json ов. Шлёт результат в канал out
func natsReceiver(m *stan_stream.Msg, out chan<- stan.Message) {
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
	fmt.Printf("[Info] Got msg from nats: type - %T\n", valid)
	// stan.MsgPrinter(m)
	out <- stan.Message{string(m.Data), &valid}
}

// Запись полученных сообщений в базу данных
func sendFromNats2DB(q *db.Query, inp <-chan stan.Message) chan stan.Message {
	db2cache := make(chan stan.Message)
	defer close(db2cache)

	go func() {
		for msg := range inp {
			_, err := q.SetOrder(msg.Json_struct)

			if err != nil {
				fmt.Printf("[Warning] Error writing to the db: %s\n", err)
				return
			}

			db2cache <- msg
			fmt.Println("[Info] Successful db entry")
		}
	}()

	return db2cache
}

func cacheLoad(e db.Engine, c *map[string]string) {
	rows, err := e.DB.Query(db.GetOrderForStr)
	defer rows.Close()

	if err != nil {
		fmt.Printf("[Warning] Failed to SELECT data for caching %s\n", err.Error())
	}

	for rows.Next() {
		var res stan.MetaRootString
		err := rows.Scan(&res.Order_uid, &res.Track_number, &res.Entry,
			&res.Internal_signature, &res.Customer_id, &res.Delivery_service,
			&res.Shardkey, &res.Sm_id, &res.Date_created, &res.Oof_shard,
			&res.Delivery, &res.Payment)
		if err != nil {
			fmt.Printf("[Warning] Failed to cache row: %s\n", err.Error())
			continue
		}

		order_items := make([]string, 0)
		itm_rows, err := e.DB.Query(stan.GetAllOrderItems, res.Order_uid)
		if err != nil {
			fmt.Printf("[Warning] Failed to cache row: %s\n", err.Error())
			continue
		}
		for itm_rows.Next() {
			var itm string
			err := itm_rows.Scan(&itm)
			if err != nil {
				fmt.Printf("[Warning] Failed to cache order item: %s\n", err.Error())
				continue
			}
			order_items = append(order_items, itm)
		}
		res.Items = &order_items
		res_serialized, _ := json.Marshal(res)
		c[res.Order_uid] = res_serialized
	}
}

// func cacheContains(key string, c *[]string) bool {

// }

func cacheControl() {

}
