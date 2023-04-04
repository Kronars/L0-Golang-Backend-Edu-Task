package stan

import (
	"fmt"
	"time"

	"github.com/nats-io/stan.go"
)

type Msg stan.Msg

// Структура для обмена между обработчиками
type Message struct {
	Json_str    string
	Json_struct *MetaRoot
}

type Handler struct {
	Callback func(m *stan.Msg, ch chan<- Message)
	Topic    string
	Out      chan Message
}

// Подключение к сереверу по имени клатера и клиента
func StanConn(clusterID, clientID string) stan.Conn {
	timeout_opt := stan.ConnectWait(time.Duration(1) * time.Minute)
	sc, err := stan.Connect(clusterID, clientID, timeout_opt)

	if err != nil {
		panic(fmt.Errorf("[Error] NATS Streaming server not found: %v", err))
	}
	fmt.Printf("[Info] Connected to Nats streaming as `%s`\n", clientID)
	return sc
}

func Sub(conn stan.Conn, h Handler) (stan.Subscription, error) {
	sc, err := conn.Subscribe(h.Topic, func(m *stan.Msg) { h.Callback(m, h.Out) },
		stan.StartWithLastReceived())

	if err != nil {
		return nil, err
	}
	fmt.Printf("[Info] Listening channel: %s\n", h.Topic)
	return sc, nil
}
