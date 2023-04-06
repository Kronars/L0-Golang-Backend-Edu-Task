package db

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"L0/stan"
	_ "github.com/lib/pq"
)

const (
	setOrderMeta = `
INSERT INTO order_meta (order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);`

	setDelivery = `
INSERT INTO delivery (order_uid, data_delivery)
VALUES ($1, $2);`

	setPayment = `
INSERT INTO payment (order_uid, data_payment)
VALUES ($1, $2);`

	setItem = `
INSERT INTO item (id_item, data_item)
VALUES ($1, $2)
ON CONFLICT (id_item) DO NOTHING;`

	setOrderItem = `
INSERT INTO order_item (id_cart, id_item)
VALUES ($1, $2);`

	getOneOrder = `
SELECT 
om.order_uid, om.track_number, om.entry, om.locale, 
om.internal_signature, om.customer_id, om.delivery_service, 
om.shardkey, om.sm_id, om.date_created, om.oof_shard, 
d.data_delivery, p.data_payment
FROM order_meta	AS om
JOIN delivery	AS d 	USING(order_uid)
JOIN payment	AS p 	USING(order_uid)
WHERE om.order_uid = $1;`

	getAllOrderItems = `
SELECT i.data_item
FROM order_meta AS om
JOIN order_item AS oi ON om.order_uid = oi.id_cart
JOIN item		AS i 	USING(id_item)
WHERE om.order_uid = $1;`

	// Следующие два запроса не компилятся и экспортируются в cache
	GetAllFullOrders = `
SELECT 
om.order_uid, om.track_number, om.entry, om.locale, 
om.internal_signature, om.customer_id, om.delivery_service, 
om.shardkey, om.sm_id, om.date_created, om.oof_shard, 
d.data_delivery, p.data_payment
FROM order_meta	AS om
JOIN delivery	AS d 	USING(order_uid)
JOIN payment	AS p 	USING(order_uid);`

	GetAllOrderItems = `
SELECT i.data_item
FROM order_meta AS om
JOIN order_item AS oi ON om.order_uid = oi.id_cart
JOIN item		AS i 	USING(id_item)
WHERE om.order_uid = $1;`
)

// Структура для сериализации
type MetaRootString struct {
	Order_uid          string             `json:"order_uid"`
	Track_number       string             `json:"track_number"`
	Entry              string             `json:"entry"`
	Locale             string             `json:"locale"`
	Internal_signature string             `json:"internal_signature"`
	Customer_id        string             `json:"customer_id"`
	Delivery_service   string             `json:"delivery_service"`
	Shardkey           string             `json:"shardkey"`
	Sm_id              int                `json:"sm_id"`
	Date_created       string             `json:"date_created"`
	Oof_shard          string             `json:"oof_shard"`
	Delivery           *string            `json:"-"` // Сюда считывается из бд
	Payment            *string            `json:"-"`
	Items              *[]string          `json:"-"`
	Delivery_json      *json.RawMessage   `json:"delivery"` // Сюда перекладывается из delivery для сериализации
	Payment_json       *json.RawMessage   `json:"payment"`
	Items_json         *[]json.RawMessage `json:"items"`
}

// Создание объекта скопилированных запросов
func MakeQuery(e Engine) *Query {
	str_stmt := []string{setOrderMeta, setDelivery, setPayment,
		setItem, setOrderItem, getOneOrder, getAllOrderItems}
	cs := map[string]*sql.Stmt{} // Compiled (sql) Statements

	for _, stmt := range str_stmt {
		prepared, err := e.DB.Prepare(stmt)
		if err != nil {
			panic(fmt.Errorf("[Error] Sql statement compilation failed:\nError: %w \nStatement: %s", err, stmt))
		}

		cs[stmt] = prepared
	}

	return &Query{&statements{cs[setOrderMeta], cs[setDelivery],
		cs[setPayment], cs[setItem], cs[setOrderItem], cs[getOneOrder], cs[getAllOrderItems]}}
}

// Встраиваение интерфейса, что бы скрыть скомпилированные запросы
type Query struct {
	CRUD
}

type CRUD interface {
	SetOrder(order *stan.MetaRoot) (uid string, err error)
	GetOrder(uid string) (order *MetaRootString, err error)
	Close()
}

// Скомпилированные SQL запросы
type statements struct {
	setOrderMeta     *sql.Stmt
	setDelivery      *sql.Stmt
	setPayment       *sql.Stmt
	setItem          *sql.Stmt
	setOrderItem     *sql.Stmt
	getOneOrder      *sql.Stmt
	getAllOrderItems *sql.Stmt
}

// Запись в бд. Поля таблиц payment, delivery, item == строковый json
// -> поэтому при записи сериализую структуры обратно в json строки
func (s *statements) SetOrder(order *stan.MetaRoot) (uid string, err error) {
	id_meta, err_m := s.writeMeta(order)
	if err_m != nil {
		return "", fmt.Errorf("failed to write meta root: %w", err_m)
	}

	err_n := s.writeNested(order)
	if err != nil {
		return "", fmt.Errorf("failed to write delivery or payment: %w", err_n)
	}

	err_i := s.writeItems(order)
	if err != nil {
		return "", fmt.Errorf("failed to write items: %w", err_i)
	}

	return id_meta, nil
}

// Запись в таблицу order_meta
func (s *statements) writeMeta(order *stan.MetaRoot) (string, error) {
	_, err := s.setOrderMeta.Exec(
		&order.Order_uid,
		&order.Track_number,
		&order.Entry,
		&order.Locale,
		&order.Internal_signature,
		&order.Customer_id,
		&order.Delivery_service,
		&order.Shardkey,
		&order.Sm_id,
		&order.Date_created,
		&order.Oof_shard,
	)

	if err != nil {
		return "", err
	}
	return order.Order_uid, nil
}

// Сериализация и запись вложенных структур delivery & payment
func (s *statements) writeNested(order *stan.MetaRoot) error {
	delivery_json, _ := json.Marshal(&order.Delivery)
	_, err_d := s.setDelivery.Exec(&order.Order_uid, string(delivery_json))

	if err_d != nil {
		return err_d
	}

	payment_json, _ := json.Marshal(&order.Payment)
	_, err_p := s.setPayment.Exec(&order.Order_uid, string(payment_json))

	if err_p != nil {
		return err_p
	}

	return nil
}

// Сериализация, запись item'ов и их айдишников в таблицу order_item (связь многие ко многим)
func (s *statements) writeItems(order *stan.MetaRoot) error {
	for _, itm := range *order.Items {
		itm_json, _ := json.Marshal(&itm)
		_, err_i := s.setItem.Exec(itm.Chrt_id, itm_json)
		if err_i != nil {
			return fmt.Errorf("error during writing to the item table: %w", err_i)
		}

		_, err_io := s.setOrderItem.Exec(&order.Order_uid, itm.Chrt_id)
		if err_io != nil {
			return fmt.Errorf("error during writing to the order_item table: %w", err_io)
		}
	}
	return nil
}

func (s *statements) GetOrder(uid string) (order *MetaRootString, err error) {
	order_row, err := s.getOneOrder.Query(uid)
	if err != nil {
		return nil, err
	}

	order_row.Next()
	order_struct, err := ScanOrder(order_row)
	if err != nil {
		return nil, err
	}

	order_items_rows, _ := s.getAllOrderItems.Query(uid)
	if err != nil {
		return nil, err
	}

	items := make([]string, 0)
	for order_items_rows.Next() {
		var item string
		_ = order_items_rows.Scan(&item)
		items = append(items, item)
	}
	order_struct.Items = &items

	return order_struct, nil
}

func ScanOrder(order_row *sql.Rows) (*MetaRootString, error) {
	var res MetaRootString
	err := order_row.Scan(&res.Order_uid, &res.Track_number, &res.Entry, &res.Locale,
		&res.Internal_signature, &res.Customer_id, &res.Delivery_service,
		&res.Shardkey, &res.Sm_id, &res.Date_created, &res.Oof_shard,
		&res.Delivery, &res.Payment)

	return &res, err
}

func (s *statements) Close() {
	s.setOrderMeta.Close()
	s.setDelivery.Close()
	s.setPayment.Close()
	s.setItem.Close()
	s.setOrderItem.Close()
	fmt.Println("[Info] Closed all db compiled statements")
}
