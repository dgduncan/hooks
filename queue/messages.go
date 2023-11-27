package queue

import "time"

type OnStarted struct {
	Timestamp time.Time
}
type OnStopped struct {
	Timestamp time.Time
}

type OnConnectAuthenticate struct {
	ClientID  string    `json:"client_id"`
	Username  string    `json:"username"`
	Timestamp time.Time `json:"timestamp"`
}

type OnPublished struct {
	ClientID  string    `json:"client_id"`
	Topic     string    `json:"topic"`
	Payload   []byte    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

type OnConnect struct {
	ClientID  string    `json:"client_id"`
	Username  string    `json:"username"`
	Timestamp time.Time `json:"timestamp"`
}

type OnDisconnect struct {
	ClientID  string    `json:"client_id"`
	Username  string    `json:"username"`
	Timestamp time.Time `json:"timestamp"`
}

type OnSessionEstablished struct {
	ClientID  string    `json:"client_id"`
	Username  string    `json:"username"`
	Timestamp time.Time `json:"timestamp"`
	Connected bool      `json:"connected"`
}

type OnSubscribed struct {
	ClientID   string    `json:"client_id"`
	Username   string    `json:"username"`
	Topic      string    `json:"topic"`
	Subscribed bool      `json:"subscribed"`
	Timestamp  time.Time `json:"timestamp"`
}

type OnUnsubscribed struct {
	ClientID   string    `json:"client_id"`
	Username   string    `json:"username"`
	Topic      string    `json:"topic"`
	Subscribed bool      `json:"subscribed"`
	Timestamp  time.Time `json:"timestamp"`
}

type OnWillSent struct {
	ClientID  string    `json:"client_id"`
	Topic     string    `json:"topic"`
	Payload   []byte    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}
