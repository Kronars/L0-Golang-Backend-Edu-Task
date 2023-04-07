package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"

	"L0/cache"
	"L0/db"
	"L0/stan"

	fiber "github.com/gofiber/fiber/v2"
	"github.com/gofiber/template/html"
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
	// Последовательная выгрузка БД в кеш
	cacheLoad(engine, cacheIndex)

	// Компиляция запросов
	q := db.MakeQuery(*engine)
	defer q.Close()

	// Инициализация веб сервера
	html_engn := html.New("./front", ".html") //
	app := fiber.New(fiber.Config{
		Views:   html_engn,
		AppName: "L0",
	})
	app.Static("/js", "./front/js")

	// ------- Конвеер --------
	// Инициализация брокера, подписки и обработчика: nats -> chan
	conn, sc, nats2db := newStanConn()
	defer sc.Close()
	defer conn.Close()

	// Обработчик: chan -> db
	db2cache := sendFromNats2DB(q, nats2db)
	defer close(nats2db)

	// Кеширование новых записей
	SendFromDB2cache(db2cache, cacheIndex)

	// ------ Веб интерфейс ------
	// AJAX запросы записей
	app.Get("/cache/:key", func(c *fiber.Ctx) error {
		key := c.Params("key")
		order_struct, err := q.GetOrder(key)
		if err != nil {
			return c.Status(fiber.StatusNotFound).SendString(err.Error())
		}
		Prepare4Ser(order_struct)
		order_string, _ := json.Marshal(order_struct)
		return c.SendString(string(order_string))
	})
	// Основная страница
	app.Get("/*", func(c *fiber.Ctx) error {
		return c.Render("index", fiber.Map{
			"Amount": len(cacheIndex),
		})
	})

	// Подъём сервера
	go app.Listen(":8080")

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
	fmt.Println("[Info] Got msg from nats.")
	// stan.MsgPrinter(m)
	out <- stan.Message{Json_str: string(m.Data), Json_struct: &valid}
}

// Запись полученных сообщений в базу данных
func sendFromNats2DB(q *db.Query, inp <-chan stan.Message) chan stan.Message {
	db2cache := make(chan stan.Message, 64)

	go func() {
		defer close(db2cache)
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

func cacheLoad(e *db.Engine, c map[string]string) {
	serializedOrders, err := cache.SerAllOrders(e)
	if err != nil {
		panic(fmt.Errorf("[Error] Failed to load cache from db: %w", err))
	}

	for uid, j_str := range serializedOrders {
		c[uid] = j_str
	}
	fmt.Println("[Info] Successful database caching")
}

// Читает канал и дополняет карту кеша
func SendFromDB2cache(in <-chan stan.Message, c map[string]string) {
	go func() {
		for msg := range in {
			_, ok := c[msg.Json_struct.Order_uid]
			if ok {
				fmt.Printf("[Warning] Attempt to cache an already known record. uid: %s\n", msg.Json_struct.Order_uid)
				continue
			} else {
				c[msg.Json_struct.Order_uid] = msg.Json_str
			}
		}
	}()
}

// Костыль что бы корректно сериализовать уже сериализованную строку
func Prepare4Ser(order_struct *db.MetaRootString) {
	del_byte := json.RawMessage([]byte(*(order_struct.Delivery)))
	pay_byte := json.RawMessage([]byte(*(order_struct.Payment)))
	order_struct.Delivery_json = &del_byte
	order_struct.Payment_json = &pay_byte

	res := make([]json.RawMessage, 0)
	for _, itm := range *order_struct.Items {
		itm_raw := json.RawMessage([]byte(itm))
		res = append(res, itm_raw)
	}
	order_struct.Items_json = &res
}
