package db

import (
	"fmt"

	_ "github.com/lib/pq"
)

const (
	createOrderMeta = `
CREATE TABLE IF NOT EXISTS order_meta (
    order_uid VARCHAR(24) PRIMARY KEY,
	UNIQUE(order_uid),
    track_number VARCHAR(255) NOT NULL,
    entry VARCHAR(255) NOT NULL,
    locale VARCHAR(255),
    internal_signature VARCHAR(255),
    customer_id VARCHAR(255) NOT NULL,
    delivery_service VARCHAR(255),
    shardkey VARCHAR(255),
    sm_id INT,
    date_created VARCHAR(255),
    oof_shard VARCHAR(255)
);`

	createDelivery = `
CREATE TABLE IF NOT EXISTS delivery (
    order_uid VARCHAR(24) PRIMARY KEY,
	FOREIGN KEY (order_uid) REFERENCES order_meta(order_uid),
    data_delivery JSON NOT NULL
);`

	createPayment = `
CREATE TABLE IF NOT EXISTS payment (
    order_uid VARCHAR(24) PRIMARY KEY,
	FOREIGN KEY (order_uid) REFERENCES order_meta(order_uid),
    data_payment JSON NOT NULL
);`

	createItem = `
CREATE TABLE IF NOT EXISTS item (
id_item INTEGER PRIMARY KEY,
data_item JSON NOT NULL
);`

	createOrderItem = `
CREATE TABLE IF NOT EXISTS order_item (
    id_cart VARCHAR(24) REFERENCES order_meta(order_uid),
	id_item INTEGER REFERENCES item(id_item),
	PRIMARY KEY (id_cart, id_item)
);`
)

func (e *Engine) CreateTables() {
	declarations := []string{createOrderMeta, createDelivery, createPayment, createItem, createOrderItem}

	for _, declare := range declarations {
		_, err := e.DB.Exec(declare)
		if err != nil {
			err = fmt.Errorf(`
[Error] Failed to create table. Reason:
%w
Query: %s`, err, declare)
			panic(err)
		}
	}
	fmt.Println("[Info] Postgres tables created")
}

// Запрос для проверки
// select order_uid,
// d.data_delivery->'name' as name,
// d.data_delivery->'city' as city,
// p.data_payment->'amount' as amount,
// i.data_item->'name' as item
// from order_meta as om
// join delivery as d using(order_uid)
// join payment as p using(order_uid)
// join order_item as oi on om.order_uid = oi.id_cart
// join item as i using(id_item);
