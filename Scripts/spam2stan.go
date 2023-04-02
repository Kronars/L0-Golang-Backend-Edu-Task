package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/nats-io/stan.go"
)

const (
	ClusterName = "test-cluster"
	ClientName  = "spammer"
	Channel     = "test"
)

func main() {
	Item_template := string(`{
    "chrt_id": %d,
    "track_number": "WBILMTESTTRACK",
    "price": 453,
    "rid": "ab4219087a764ae0btest",
    "name": "Mascaras",
    "sale": 30,
    "size": "0",
    "total_price": 317,
    "nm_id": 2389212,
    "brand": "Vivienne Sabo",
    "status": 20
},`)

	Order_template := string(`
{
	"order_uid": "%s",
	"track_number": "WBILMTESTTRACK",
	"entry": "WBIL",
	"delivery": {
		"name": "Test Testov",
		"phone": "+9720000000",
		"zip": "2639809",
		"city": "Kiryat Mozkin",
		"address": "Ploshad Mira 15",
		"region": "Kraiot",
		"email": "test@gmail.com"
	},
	"payment": {
		"transaction": "b563feb7b2b84b6test",
		"request_id": "",
		"currency": "USD",
		"provider": "wbpay",
		"amount": 1817,
		"payment_dt": 1637907727,
		"bank": "alpha",
		"delivery_cost": 1500,
		"goods_total": 317,
		"custom_fee": 0
	},
	"items": [
        %s
		{
			"chrt_id": 9934930,
			"track_number": "WBILMTESTTRACK",
			"price": 453,
			"rid": "ab4219087a764ae0btest",
			"name": "Mascaras",
			"sale": 30,
			"size": "0",
			"total_price": 317,
			"nm_id": 2389212,
			"brand": "Vivienne Sabo",
			"status": 202
		}
	],
	"locale": "en",
	"internal_signature": "",
	"customer_id": "test",
	"delivery_service": "meest",
	"shardkey": "9",
	"sm_id": 99,
	"date_created": "2021-11-26T06:22:19Z",
	"oof_shard": "1"
}`)

	// Инициализация Nats
	conn := StanConn(ClusterName, ClientName)
	defer conn.Close()

	for i := 0; i < 10; i++ {
		prepared_items := genItems(rand.Intn(3), Item_template)
		prepared_order := fmt.Sprintf(Order_template, randString(20), prepared_items)
		conn.Publish(Channel, []byte(prepared_order))
		fmt.Printf("Send %d msg's\n", i)
	}

}

func genItems(amount int, temlate string) string {
	prepared := ""
	for i := 0; i < amount; i++ {
		id := rand.Intn(9999999) + 1000000
		prepared += fmt.Sprintf(temlate, id)
	}
	return prepared
}

func randString(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789")
	result := make([]rune, length)
	for i := range result {
		result[i] = letters[rand.Intn(len(letters))]
	}
	res := string(result)
	res += "b2" // ранил
	return res
}

func StanConn(clusterID, clientID string) stan.Conn {
	timeout_opt := stan.ConnectWait(time.Duration(1) * time.Minute)
	sc, err := stan.Connect(clusterID, clientID, timeout_opt)

	if err != nil {
		log.Fatalf("[Error] NATS Streaming server not found: %v", err)
	}
	fmt.Println("[Info] Connected to Nats streaming")
	return sc
}
