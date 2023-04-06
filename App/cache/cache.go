package cache

import (
	"L0/db"
	"encoding/json"
	"fmt"
)

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
func getAllOrders(e *db.Engine) ([]*db.MetaRootString, error) {
	orders, err := e.DB.Query(db.GetAllFullOrders)
	if err != nil {
		return nil, err
	}
	defer orders.Close()

	ordersStruct := make([]*db.MetaRootString, 0)

	for orders.Next() {
		res, err := db.ScanOrder(orders)
		if err != nil {
			fmt.Printf("[Warning] Failed to cache row: %s\n", err.Error())
			continue
		}
		ordersStruct = append(ordersStruct, res)
	}
	return ordersStruct, nil
}

// Получение товаров каждого заказа. Заполняет оставшиееся поле items структуры
func catItems2orders(e *db.Engine, cropOrders []*db.MetaRootString) ([]*db.MetaRootString, error) {
	for _, order := range cropOrders {
		itm_rows, err := e.DB.Query(db.GetAllOrderItems, order.Order_uid)
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
func serializeOrders(orders []*db.MetaRootString) (map[string]string, error) {
	serialized := make(map[string]string, 0)
	for _, order := range orders {
		res, err := json.Marshal(order)
		if err != nil {
			return nil, err
		}
		serialized[order.Order_uid] = string(res)
	}
	return serialized, nil
}
