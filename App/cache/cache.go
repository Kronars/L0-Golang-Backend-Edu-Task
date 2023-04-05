package cache

import (
	"L0/db"
	"encoding/json"
	"fmt"
)

const (
	GetOrderForStr = `
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

// PREPARE test (text) AS
// 	SELECT i.data_item
// 	FROM order_meta AS om
// 	JOIN order_item AS oi ON om.order_uid = oi.id_cart
// 	JOIN item		AS i 	USING(id_item)
// 	WHERE om.order_uid = $1;

// Структура для преобразования в
type metaRootString struct {
	order_uid          string
	track_number       string
	entry              string
	locale             string
	internal_signature string
	customer_id        string
	delivery_service   string
	shardkey           string
	sm_id              int
	date_created       string
	oof_shard          string
	Delivery           *string   `json:"delivery"`
	Payment            *string   `json:"payment"`
	Items              *[]string `json:"items"`
}

// По скольку заказы и товары хранятся отдельно:
// селект всех заказов -> селект всех товаров каждого заказа -> сериализация -> кеширование
func SerAllOrders(e *db.Engine) (map[string]string, error) {
	// Струкутра со всеми полями кроме товаров
	cropOrders, err := getAllOrders(e)
	if err != nil {
		return nil, fmt.Errorf("getAllOrders: failed to SELECT orders data for caching %w", err)
	}
	// Структура с заполненными товарами
	fullOrders, err := catItems2orders(e, cropOrders)
	if err != nil {
		return nil, fmt.Errorf("catItems2orders: failed to SELECT items data for caching %w", err)
	}
	// Срез из сериализованных структур
	strOrders, err := serializeOrders(fullOrders)
	if err != nil {
		return nil, fmt.Errorf("serializeOrders: failed to serialize order struct %w", err)
	}

	return strOrders, nil
}

// Получение всех заказов, поле items - пустое
func getAllOrders(e *db.Engine) ([]*metaRootString, error) {
	orders, err := e.DB.Query(GetOrderForStr)
	if err != nil {
		return nil, err
	}
	defer orders.Close()

	ordersStruct := make([]*metaRootString, 0)

	for orders.Next() {
		var res metaRootString
		err := orders.Scan(&res.order_uid, &res.track_number, &res.entry, &res.locale,
			&res.internal_signature, &res.customer_id, &res.delivery_service,
			&res.shardkey, &res.sm_id, &res.date_created, &res.oof_shard,
			&res.Delivery, &res.Payment)
		if err != nil {
			fmt.Printf("[Warning] Failed to cache row: %s\n", err.Error())
			continue
		}
		ordersStruct = append(ordersStruct, &res)
	}
	return ordersStruct, nil
}

// Получение товаров каждого заказа. Заполняет оставшиееся поле items структуры
func catItems2orders(e *db.Engine, cropOrders []*metaRootString) ([]*metaRootString, error) {
	for _, order := range cropOrders {
		itm_rows, err := e.DB.Query(GetAllOrderItems, order.order_uid)
		if err != nil {
			return cropOrders, err // Типа если у заказа отвалилсь все товары - полная отмена
		}
		defer itm_rows.Close()

		order_items := make([]string, 0)

		for itm_rows.Next() {
			var itm string
			err := itm_rows.Scan(&itm)
			if err != nil {
				fmt.Printf("[Warning] Failed to cache order item: %s\n", err.Error()) // А если отвалился только один товар, то жить можно
				continue
			}
			order_items = append(order_items, itm)
		}

		order.Items = &order_items
	}
	return cropOrders, nil
}

// Проходит по списку, сериализует структуры в строки
func serializeOrders(orders []*metaRootString) (map[string]string, error) {
	serialized := make(map[string]string, 0)
	for _, order := range orders {
		res, err := json.Marshal(order)
		if err != nil {
			return nil, err
		}
		serialized[order.order_uid] = string(res)
	}
	return serialized, nil
}
