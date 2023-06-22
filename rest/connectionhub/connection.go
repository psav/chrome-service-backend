package connectionhub

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"log"
)

type Connection struct {
	Ws   *websocket.Conn
	ID   string
	Send chan []byte
}

// This is all moved around due to circular dependencies
// If we stick with this solution, we can fix it properly.
func StartKafkaReader(r *kafka.Reader) {
	defer r.Close()
	for {
		logrus.Infoln("Reading from ", r.Config().Topic)
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			logrus.Errorln("Error reading message: ", err)
			break
		}
		logrus.Infoln(fmt.Sprintf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value)))

		var p KafkaEnvelope
		err = json.Unmarshal(m.Value, &p)
		if err != nil {
			logrus.Errorln(fmt.Sprintf("Unable to unmarshal message %s\n", string(m.Value)))
		} else if p.Data.Payload == nil {
			logrus.Errorln(fmt.Sprintf("No message will be emitted do to missing payload %s! Message might not follow cloud events spec.\n", string(m.Value)))
		} else {
			event := WrapPayload(p.Data.Payload, p.Source, p.Id, p.Type)
			event.Time = p.Time
			data, err := json.Marshal(event)
			if err != nil {
				log.Println("Unable to marshal payload data", p, err)
			} else {
				validateErr := ValidatePayload(p)
				if validateErr == nil {
					newMessage := Message{
						Destinations: MessageDestinations{
							Users:         p.Data.Users,
							Roles:         p.Data.Roles,
							Organizations: p.Data.Organizations,
						},
						Broadcast: p.Data.Broadcast,
						Data:      data,
					}
					if p.Data.Broadcast {
						logrus.Infoln("Emitting new broadcast message from kafka reader: ", string(newMessage.Data))
						ConnectionHub.Broadcast <- newMessage
					} else {
						logrus.Infoln("Emitting new message from kafka reader: ", string(newMessage.Data))
						ConnectionHub.Emit <- newMessage
					}
				} else {
					logrus.Errorln(validateErr)
				}
			}
		}
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
