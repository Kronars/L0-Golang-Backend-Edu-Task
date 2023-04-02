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

	// ! Одинаковые айтемы дублируется из за SERIAL PRIMARY KEY
	// TODO: инсёрт ор игнор, при заказе разными людьми одного товара, запись в бд должна либо обновлятся либо игнорироваться
	setItem = `
INSERT INTO item (id_item, data_item)
VALUES ($1, $2);`

	setOrderItem = `
INSERT INTO order_item (id_cart, id_item)
VALUES ($1, $2);`

	// ! Это заглушка - дописать
// 	getOrder = `
// SELECT * FROM order_meta
// WHERE order_uid = $1
// FROM order_meta;`
)

func MakeQuery(e Engine) *Query {
	str_stmt := []string{setOrderMeta, setDelivery, setPayment, setItem, setOrderItem}
	cs := map[string]*sql.Stmt{} // Compiled (sql) Statements

	for _, stmt := range str_stmt {
		prepared, err := e.DB.Prepare(stmt)
		if err != nil {
			panic(fmt.Errorf("[Error] Sql statement compilation failed:\nError: %w \nStatement: %s", err, stmt))
		}

		cs[stmt] = prepared
	}

	return &Query{&statements{cs[setOrderMeta], cs[setDelivery], cs[setPayment], cs[setItem], cs[setOrderItem]}}
}

// Встраиваение интерфейса, что бы скрыть скомпилированные запросы
type Query struct {
	CRUD
}

type CRUD interface {
	SetOrder(order *stan.MetaRoot) (uid string, err error)
	GetOrder(uid string) (order *stan.MetaRoot, err error)
	Close()
}

// Скомпилированные SQL запросы
type statements struct {
	setOrderMeta *sql.Stmt
	setDelivery  *sql.Stmt
	setPayment   *sql.Stmt
	setItem      *sql.Stmt
	setOrderItem *sql.Stmt
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

func (s *statements) GetOrder(uid string) (order *stan.MetaRoot, err error) {
	return &stan.MetaRoot{}, nil
}

func (s *statements) Close() {
	s.setOrderMeta.Close()
	s.setDelivery.Close()
	s.setPayment.Close()
	s.setItem.Close()
	s.setOrderItem.Close()
	fmt.Println("[Info] Closed all db compiled statements")
}
