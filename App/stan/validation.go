package stan

import (
	"encoding/json"
	"fmt"

	validator "github.com/go-playground/validator/v10"
	"github.com/nats-io/stan.go"
)

type MetaRoot struct {
	Order_uid          string    `validate:"required,alphanum" json:"order_uid"`
	Track_number       string    `validate:"required" json:"track_number"`
	Entry              string    `validate:"required" json:"entry"`
	Delivery           *Delivery `validate:"required" json:"delivery"`
	Payment            *Payment  `validate:"required" json:"payment"`
	Items              *[]Item   `validate:"required" json:"items"`
	Locale             string    `validate:"omitempty" json:"locale"`
	Internal_signature string    `validate:"omitempty" json:"internal_signature"`
	Customer_id        string    `validate:"required" json:"customer_id"`
	Delivery_service   string    `validate:"omitempty" json:"delivery_service"`
	Shardkey           string    `validate:"omitempty" json:"shardkey"`
	Sm_id              int       `validate:"omitempty" json:"sm_id"`
	Date_created       string    `validate:"omitempty" json:"date_created"`
	Oof_shard          string    `validate:"omitempty" json:"oof_shard"`
}

type Delivery struct {
	Name    string `validate:"required" json:"name"`
	Phone   string `validate:"required,e164" json:"phone"`
	Zip     string `validate:"omitempty" json:"zip"`
	City    string `validate:"required" json:"city"`
	Address string `validate:"required" json:"address"`
	Region  string `validate:"omitempty" json:"region"`
	Email   string `validate:"email" json:"email"`
}

type Payment struct {
	Transaction   string `validate:"required" json:"transaction"`
	Request_id    string `validate:"omitempty" json:"request_id"`
	Currency      string `validate:"iso4217" json:"currency"`
	Provider      string `validate:"required" json:"provider"`
	Amount        uint   `validate:"required" json:"amount"`
	Payment_dt    uint   `validate:"omitempty" json:"payment_dt"`
	Bank          string `validate:"omitempty" json:"bank"`
	Delivery_cost uint   `validate:"omitempty" json:"delivery_cost"`
	Goods_total   uint   `validate:"omitempty" json:"goods_total"`
	Custom_fee    uint   `validate:"omitempty" json:"custom_fee"`
}

// Лень заполнять валидацию
type Item struct {
	Chrt_id      uint   `validate:"omitempty" json:"chrt_id"`
	Track_number string `validate:"omitempty" json:"track_number"`
	Price        uint   `validate:"omitempty" json:"price"`
	Rid          string `validate:"omitempty" json:"rid"`
	Name         string `validate:"omitempty" json:"name"`
	Sale         int    `validate:"omitempty" json:"sale"`
	Size         string `validate:"omitempty" json:"size"`
	Total_price  uint   `validate:"omitempty" json:"total_price"`
	Nm_id        int    `validate:"omitempty" json:"nm_id"`
	Brand        string `validate:"omitempty" json:"brand"`
	Status       int    `validate:"omitempty" json:"status"`
}

func Parse2Struct(m *stan.Msg) (MetaRoot, error) {
	raw_json := m.Data

	var parsed MetaRoot
	err := json.Unmarshal(raw_json, &parsed)
	if err != nil {
		return MetaRoot{}, fmt.Errorf("parsing error: %w", err)
	}
	return parsed, nil
}

func Validate(data MetaRoot) (MetaRoot, error) {
	validate := validator.New()
	err := validate.Struct(data)
	if err != nil {
		return MetaRoot{}, fmt.Errorf("validation error: %w", err)
	}
	return data, nil
}

func MsgPrinter(m *stan.Msg) {
	msg := string(m.Data)
	if len(msg) > 40 {
		repl := msg[:20]
		repl += "... ..."
		repl += msg[len(msg)-20:]
		fmt.Printf("[Info] Recive msg: %s\n", repl)
	} else {
		fmt.Printf("[Info] Recive msg: %s\n", msg)
	}
}
