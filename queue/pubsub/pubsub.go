package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"cloud.google.com/go/pubsub"
	messages "github.com/mochi-mqtt/hooks/queue"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

type Hook struct {
	onStartedTopic             *pubsub.Topic
	onStoppedTopic             *pubsub.Topic
	onConnectAuthenticateTopic *pubsub.Topic
	onACLCheckTopic            *pubsub.Topic
	// onConnectTopic            *pubsub.Topic
	// onDisconnectTopic         *pubsub.Topic
	// onSessionEstablishedTopic *pubsub.Topic
	// onPublishedTopic          *pubsub.Topic
	// onSubscribedTopic         *pubsub.Topic
	// onUnsubscribedTopic       *pubsub.Topic
	// onWillSentTopic           *pubsub.Topic
	ignoreList []string
	mqtt.HookBase
}

type Options struct {
	OnStartedTopic             *pubsub.Topic
	OnStoppedTopic             *pubsub.Topic
	OnConnectAuthenticateTopic *pubsub.Topic
	OnACLCheckTopic            *pubsub.Topic
	// OnConnectTopic            *pubsub.Topic
	// OnDisconnectTopic         *pubsub.Topic
	// OnSessionEstablishedTopic *pubsub.Topic
	// OnPublishedTopic          *pubsub.Topic
	// OnSubscribedTopic         *pubsub.Topic
	// OnUnubscribedTopic        *pubsub.Topic
	// OnWillSentTopic           *pubsub.Topic
	IgnoreList []string
}

func (pmh *Hook) ID() string {
	return "queue-pubsub-hook"
}

func (pmh *Hook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnStarted,
		mqtt.OnStopped,
		mqtt.OnConnectAuthenticate,
		mqtt.OnACLCheck,
		// mqtt.OnConnect,
		// mqtt.OnDisconnect,
		// mqtt.OnSessionEstablished,
		// mqtt.OnPublished,
		// mqtt.OnSubscribed,
		// mqtt.OnUnsubscribed,
		// mqtt.OnWillSent,
	}, []byte{b})
}

func (pmh *Hook) Init(config any) error {
	if config == nil {
		return errors.New("nil config")
	}

	pmhc, ok := config.(Options)
	if !ok {
		return errors.New("improper config")
	}

	if pmhc.IgnoreList == nil {
		pmh.Log.Debug("nil ignoreList, creating empty slice")
		pmhc.IgnoreList = make([]string, 0) // would this be better as a map?
	}

	pmh.onStartedTopic = pmhc.OnStartedTopic
	pmh.onStoppedTopic = pmhc.OnStoppedTopic
	pmh.onConnectAuthenticateTopic = pmhc.OnConnectAuthenticateTopic
	pmh.onACLCheckTopic = pmhc.OnACLCheckTopic
	// pmh.onConnectTopic = pmhc.OnConnectTopic
	// pmh.onDisconnectTopic = pmhc.OnDisconnectTopic
	// pmh.onSessionEstablishedTopic = pmhc.OnSessionEstablishedTopic
	// pmh.onPublishedTopic = pmhc.OnPublishedTopic
	// pmh.onSubscribedTopic = pmhc.OnSubscribedTopic
	// pmh.onUnsubscribedTopic = pmhc.OnUnubscribedTopic
	// pmh.onWillSentTopic = pmhc.OnWillSentTopic
	// pmh.disallowlist = pmhc.DisallowList

	return nil
}

func (pmh *Hook) OnStarted() {
	if pmh.onStartedTopic == nil {
		pmh.Log.Debug("onStartedTopic is nil, returning early")
		return
	}

	if err := publish(pmh.onStartedTopic, messages.OnStarted{
		Timestamp: time.Now().UTC(),
	}); err != nil {
		pmh.Log.Error("error publishing OnStarted message to topic", "error", err)
	}
}

func (pmh *Hook) OnStopped() {
	if pmh.onStoppedTopic == nil {
		pmh.Log.Debug("onStoppedTopic is nil, returning early")
		return
	}

	if err := publish(pmh.onStoppedTopic, messages.OnStopped{
		Timestamp: time.Now().UTC(),
	}); err != nil {
		pmh.Log.Error("error publishing OnStopped message to topic", "error", err)
	}
}

func (pmh *Hook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	if pmh.onConnectAuthenticateTopic == nil {
		pmh.Log.Debug("onConnectAuthenticateTopic is nil, returning early")
		return true
	}

	if pmh.checkIgnored(string(cl.Properties.Username)) {
		pmh.Log.Debug("username is ignored, returning early")
		return true
	}

	return true
}

func (pmh *Hook) OnACLCheck(cl *mqtt.Client, pk packets.Packet) bool {
	if pmh.onConnectAuthenticateTopic == nil {
		pmh.Log.Debug("onConnectAuthenticateTopic is nil, returning early")
		return true
	}

	if pmh.checkIgnored(string(cl.Properties.Username)) {
		pmh.Log.Debug("username is ignored, returning early")
		return true
	}

	return true
}

// func (pmh *PubsubMessagingHook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
// 	if pmh.onUnsubscribedTopic == nil {
// 		return
// 	}

// 	if !pmh.checkAllowed(string(cl.Properties.Username)) {
// 		return
// 	}

// 	if err := publish(pmh.onUnsubscribedTopic, OnSubscribedMessage{
// 		ClientID:   cl.ID,
// 		Username:   string(cl.Properties.Username),
// 		Timestamp:  time.Now(),
// 		Subscribed: false,
// 		Topic:      pk.TopicName,
// 	}); err != nil {
// 		// pmh.Log.Err(err).Msg("")
// 	}
// }

// func (pmh *PubsubMessagingHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
// 	if pmh.onSubscribedTopic == nil {
// 		return
// 	}

// 	if !pmh.checkAllowed(string(cl.Properties.Username)) {
// 		return
// 	}

// 	if err := publish(pmh.onSubscribedTopic, OnSubscribedMessage{
// 		ClientID:   cl.ID,
// 		Username:   string(cl.Properties.Username),
// 		Timestamp:  time.Now(),
// 		Subscribed: true,
// 		Topic:      pk.TopicName,
// 	}); err != nil {
// 		// pmh.Log.Err(err).Msg("")
// 	}
// }

// func (pmh *PubsubMessagingHook) OnConnect(cl *mqtt.Client, pk packets.Packet) {
// 	if pmh.onConnectTopic == nil {
// 		return
// 	}

// 	if !pmh.checkAllowed(string(cl.Properties.Username)) {
// 		return
// 	}

// 	if err := publish(pmh.onConnectTopic, OnConnectMessage{
// 		ClientID:  cl.ID,
// 		Username:  string(cl.Properties.Username),
// 		Timestamp: time.Now(),
// 	}); err != nil {
// 		// pmh.Log.Err(err).Msg("")
// 	}
// }

// func (pmh *PubsubMessagingHook) OnSessionEstablished(cl *mqtt.Client, pk packets.Packet) {
// 	if pmh.onSessionEstablishedTopic == nil {
// 		return
// 	}

// 	if !pmh.checkAllowed(string(cl.Properties.Username)) {
// 		return
// 	}

// 	if err := publish(pmh.onSessionEstablishedTopic, OnSessionEstablishedMessage{
// 		ClientID:  cl.ID,
// 		Username:  string(cl.Properties.Username),
// 		Timestamp: time.Now(),
// 		Connected: true,
// 	}); err != nil {
// 		// pmh.Log.Err(err).Msg("")
// 	}
// }

// func (pmh *PubsubMessagingHook) OnDisconnect(cl *mqtt.Client, connect_err error, expire bool) {
// 	if pmh.onDisconnectTopic == nil {
// 		return
// 	}

// 	if !pmh.checkAllowed(string(cl.Properties.Username)) {
// 		return
// 	}

// 	if err := publish(pmh.onDisconnectTopic, OnDisconnectMessage{
// 		ClientID:  cl.ID,
// 		Username:  string(cl.Properties.Username),
// 		Timestamp: time.Now(),
// 	}); err != nil {
// 		// pmh.Log.Err(err).Msg("")
// 	}
// }

// func (pmh *PubsubMessagingHook) OnPublished(cl *mqtt.Client, pk packets.Packet) {
// 	if pmh.onPublishedTopic == nil {
// 		return
// 	}

// 	if !pmh.checkAllowed(string(cl.Properties.Username)) {
// 		return
// 	}

// 	if err := publish(pmh.onPublishedTopic, OnPublishedMessage{
// 		ClientID:  cl.ID,
// 		Topic:     pk.TopicName,
// 		Payload:   pk.Payload,
// 		Timestamp: time.Now(),
// 	}); err != nil {
// 		// pmh.Log.Err(err).Msg("")
// 	}
// }

// func (pmh *PubsubMessagingHook) OnWillSent(cl *mqtt.Client, pk packets.Packet) {
// 	if pmh.onWillSentTopic == nil {
// 		return
// 	}

// 	if !pmh.checkAllowed(string(cl.Properties.Username)) {
// 		return
// 	}

// 	if err := publish(pmh.onWillSentTopic, OnWillSentMessage{
// 		ClientID:  cl.ID,
// 		Topic:     pk.TopicName,
// 		Payload:   pk.Payload,
// 		Timestamp: time.Now(),
// 	}); err != nil {
// 		// pmh.Log.Err(err).Msg("")
// 	}
// }

// func (pmh *PubsubMessagingHook) checkAllowed(username string) bool {
// 	for _, disallowedUsername := range pmh.disallowlist {
// 		if username == disallowedUsername {
// 			return false
// 		}
// 	}
// 	return true
// }

func (pmh *Hook) checkIgnored(username string) bool {
	for _, ignoredUsername := range pmh.ignoreList {
		if username == ignoredUsername {
			return true
		}
	}
	return false
}

func publish(topic *pubsub.Topic, data any) error {
	b, _ := json.Marshal(data)

	// TODO : add options to store response for later
	topic.Publish(context.TODO(), &pubsub.Message{
		Data: b,
	})

	return nil
}
