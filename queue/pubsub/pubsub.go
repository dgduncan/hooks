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
	"github.com/mochi-mqtt/server/v2/system"
)

type Hook struct {
	onStartedTopic              *pubsub.Topic
	onStoppedTopic              *pubsub.Topic
	onConnectAuthenticateTopic  *pubsub.Topic
	onACLCheckTopic             *pubsub.Topic
	onSysInfoTickTopic          *pubsub.Topic
	onConnectTopic              *pubsub.Topic
	onSessionEstablishTopic     *pubsub.Topic
	onSessionEstablishedTopic   *pubsub.Topic
	onDisconnectTopic           *pubsub.Topic
	onAuthPacketTopic           *pubsub.Topic
	onPacketReadTopic           *pubsub.Topic
	onPacketEncodeTopic         *pubsub.Topic
	onPacketSentTopic           *pubsub.Topic
	onPacketProcessedTopic      *pubsub.Topic
	onSubscribeTopic            *pubsub.Topic
	onSubscribedTopic           *pubsub.Topic
	onSelectSubscribersTopic    *pubsub.Topic
	onUnsubscribeTopic          *pubsub.Topic
	onUnsubscribedTopic         *pubsub.Topic
	onPublishTopic              *pubsub.Topic
	onPublishedTopic            *pubsub.Topic
	onPublishDroppedTopic       *pubsub.Topic
	onRetainMessageTopic        *pubsub.Topic
	onRetainPublishTopic        *pubsub.Topic
	onQosPublishTopic           *pubsub.Topic
	onQosCompleteTopic          *pubsub.Topic
	onQosDroppedTopic           *pubsub.Topic
	onPacketIDExhaustedTopic    *pubsub.Topic
	onWillTopic                 *pubsub.Topic
	onWillSentTopic             *pubsub.Topic
	onClientExpiredTopic        *pubsub.Topic
	onRetainedExpiredTopic      *pubsub.Topic
	storedClientsTopic          *pubsub.Topic
	storedSubscriptionsTopic    *pubsub.Topic
	storedInflightMessagesTopic *pubsub.Topic
	storedRetainedMessagesTopic *pubsub.Topic
	storedSysInfoTopic          *pubsub.Topic

	ignoreList []string
	mqtt.HookBase
}

type Options struct {
	OnStartedTopic              *pubsub.Topic
	OnStoppedTopic              *pubsub.Topic
	OnConnectAuthenticateTopic  *pubsub.Topic
	OnACLCheckTopic             *pubsub.Topic
	OnSysInfoTick               *pubsub.Topic
	OnConnectTopic              *pubsub.Topic
	OnSessionEstablishTopic     *pubsub.Topic
	OnSessionEstablishedTopic   *pubsub.Topic
	OnDisconnectTopic           *pubsub.Topic
	OnAuthPacketTopic           *pubsub.Topic
	OnPacketReadTopic           *pubsub.Topic
	OnPacketEncodeTopic         *pubsub.Topic
	OnPacketSentTopic           *pubsub.Topic
	OnPacketProcessedTopic      *pubsub.Topic
	OnSubscribeTopic            *pubsub.Topic
	OnSubscribedTopic           *pubsub.Topic
	OnSelectSubscribersTopic    *pubsub.Topic
	OnUnsubscribeTopic          *pubsub.Topic
	OnUnsubscribedTopic         *pubsub.Topic
	OnPublishTopic              *pubsub.Topic
	OnPublishedTopic            *pubsub.Topic
	OnPublishDroppedTopic       *pubsub.Topic
	OnRetainMessageTopic        *pubsub.Topic
	OnRetainPublishTopic        *pubsub.Topic
	OnQosPublishTopic           *pubsub.Topic
	OnQosCompleteTopic          *pubsub.Topic
	OnQosDroppedTopic           *pubsub.Topic
	OnPacketIDExhaustedTopic    *pubsub.Topic
	OnWillTopic                 *pubsub.Topic
	OnWillSentTopic             *pubsub.Topic
	OnClientExpiredTopic        *pubsub.Topic
	OnRetainedExpiredTopic      *pubsub.Topic
	StoredClientsTopic          *pubsub.Topic
	StoredSubscriptionsTopic    *pubsub.Topic
	StoredInflightMessagesTopic *pubsub.Topic
	StoredRetainedMessagesTopic *pubsub.Topic
	StoredSysInfoTopic          *pubsub.Topic
	IgnoreList                  []string
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
		mqtt.OnSysInfoTick,
		mqtt.OnConnect,
		mqtt.OnSessionEstablish,
		mqtt.OnSessionEstablished,
		mqtt.OnDisconnect,
		mqtt.OnAuthPacket,
		mqtt.OnPacketRead,
		mqtt.OnPacketEncode,
		mqtt.OnPacketSent,
		mqtt.OnPacketProcessed,
		mqtt.OnSubscribe,
		mqtt.OnSubscribed,
		mqtt.OnSelectSubscribers,
		mqtt.OnUnsubscribe,
		mqtt.OnUnsubscribed,
		mqtt.OnPublish,
		mqtt.OnPublished,
		mqtt.OnPublishDropped,
		mqtt.OnRetainMessage,
		mqtt.OnRetainPublished,
		mqtt.OnQosPublish,
		mqtt.OnQosComplete,
		mqtt.OnQosDropped,
		mqtt.OnPacketIDExhausted,
		mqtt.OnWill,
		mqtt.OnWillSent,
		mqtt.OnClientExpired,
		mqtt.OnRetainedExpired,
		mqtt.StoredClients,
		mqtt.StoredSubscriptions,
		mqtt.StoredInflightMessages,
		mqtt.StoredRetainedMessages,
		mqtt.StoredSysInfo,
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
	pmh.onSysInfoTick = pmhc.OnSysInfoTick
	pmh.onConnectTopic = pmhc.OnConnectTopic
	pmh.onSessionEstablishTopic = pmhc.OnSessionEstablishTopic
	pmh.onSessionEstablishedTopic = pmhc.OnSessionEstablishedTopic
	pmh.onDisconnectTopic = pmhc.OnDisconnectTopic
	pmh.onAuthPacketTopic = pmhc.OnAuthPacketTopic
	pmh.onPacketReadTopic = pmhc.OnPacketReadTopic
	pmh.onPacketEncodeTopic = pmhc.OnPacketEncodeTopic
	pmh.onPacketSentTopic = pmhc.OnPacketSentTopic
	pmh.onPacketProcessedTopic = pmhc.OnPacketProcessedTopic
	pmh.onSubscribeTopic = pmhc.OnSubscribeTopic
	pmh.onSubscribedTopic = pmhc.OnSubscribedTopic
	pmh.onSelectSubscribersTopic = pmhc.OnSelectSubscribersTopic
	pmh.onUnsubscribeTopic = pmhc.OnUnsubscribeTopic
	pmh.onUnsubscribedTopic = pmhc.OnUnsubscribedTopic
	pmh.onPublishTopic = pmhc.OnPublishTopic
	pmh.onPublishedTopic = pmhc.OnPublishedTopic
	pmh.onPublishDroppedTopic = pmhc.OnPublishDroppedTopic
	pmh.onRetainMessageTopic = pmhc.OnRetainMessageTopic
	pmh.onRetainPublishTopic = pmhc.OnRetainPublishTopic
	pmh.onQosPublishTopic = pmhc.OnQosPublishTopic
	pmh.onQosCompleteTopic = pmhc.OnQosCompleteTopic
	pmh.onQosDroppedTopic = pmhc.OnQosDroppedTopic
	pmh.onPacketIDExhaustedTopic = pmhc.OnPacketIDExhaustedTopic
	pmh.onWillTopic = pmhc.OnWillTopic
	pmh.onWillSentTopic = pmhc.OnWillSentTopic
	pmh.onClientExpiredTopic = pmhc.OnClientExpiredTopic
	pmh.onRetainedExpiredTopic = pmhc.OnRetainedExpiredTopic
	pmh.storedClientsTopic = pmhc.StoredClientsTopic
	pmh.storedSubscriptionsTopic = pmhc.StoredSubscriptionsTopic
	pmh.storedInflightMessagesTopic = pmhc.StoredInflightMessagesTopic
	pmh.storedRetainedMessagesTopic = pmhc.StoredRetainedMessagesTopic
	pmh.storedSysInfoTopic = pmhc.StoredSysInfoTopic

	pmh.ignoreList = pmhc.IgnoreList

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
		return true
	}

	return true
}

func (pmh *Hook) OnSysInfoTick(sys *system.Info) {
	if pmh.onSysInfoTickTopic == nil {
		pmh.Log.Debug("onSysInfoTickTopic is nil, returning early")
		return
	}

	return
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

func (pmh *Hook) OnConnect(cl *mqtt.Client, pk packets.Packet) {
	if pmh.onConnectTopic == nil {
		pmh.Log.Debug("onConnectTopic is nil, returning early")
		return
	}

	if pmh.checkIgnored(string(cl.Properties.Username)) {
		return
	}

	if err := publish(pmh.onConnectTopic, messages.OnConnect{
		ClientID:  cl.ID,
		Username:  string(cl.Properties.Username),
		Timestamp: time.Now(),
	}); err != nil {
		// pmh.Log.Err(err).Msg("")
	}
}

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
			pmh.Log.Debug("username is ignored, returning early")
			return true
		}
	}
	return false
}

func publish(topic *pubsub.Topic, data any) error {
	b, _ := json.Marshal(data)

	// TODO : add options to store response for later
	result := topic.Publish(context.TODO(), &pubsub.Message{
		Data: b,
	})

	result.Get()

	return nil
}
