package db

import (
	"database/sql"
	"fmt"

	"L0/stan"
	_ "github.com/lib/pq"
)

// ! Разделить на отдельные запросы, сыпет ошибкой
const (
	setOrder = `
INSERT INTO order_meta (order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);

INSERT INTO delivery (order_uid, data_delivery)
VALUES ($1, $12);

INSERT INTO payment (order_uid, data_payment)
VALUES ($1, $13);`

	setItem = `
INSERT INTO item (data_item)
VALUES ($1);

INSERT INTO order_item (id_cart, id_item)
VALUES ($2, $3)
`

	// ! Это заглушка - дописать
	getOrder = `
INSERT INTO item (data_item)
VALUES ($1);

INSERT INTO order_item (id_cart, id_item)
VALUES ($2, $3)
`
)

func CompileStmt(e Engine) Statements {
	setOrd, err_1 := e.DB.Prepare(setOrder)
	getOrd, err_2 := e.DB.Prepare(getOrder)
	setItem, err_3 := e.DB.Prepare(setItem)

	if err_1 != nil || err_2 != nil || err_3 != nil {
		panic(fmt.Errorf("\n[Error] Query compilation failed: %w\n%w\n%w", err_1, err_2, err_3))
	}

	return Statements{*setOrd, *getOrd, *setItem}
}

type Statements struct {
	SetOrder sql.Stmt
	GetOrder sql.Stmt
	SetItem  sql.Stmt
}

func (s *Statements) CloseStmt() {
	s.SetOrder.Close()
	s.SetItem.Close()
	s.GetOrder.Close()
}

func (s *Statements) SetFullOrder(m *stan.MetaRoot) (id int, err error) {
	// TODO: захардкодить запись в бд....
	// res, err := s.SetOrder.Exec(m...)

	// id, _ := res.RowsAffected()
	// return id, nil
	return 0, nil
}
