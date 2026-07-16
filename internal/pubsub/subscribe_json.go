package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType string

const (
	Ack         AckType = "Ack"
	NackRequeue AckType = "NackRequeue"
	NackDiscard AckType = "NackDiscard"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {

	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue, %v", err)
	}

	deliveries, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not deliver messages, %v", err)
	}

	go func() {
		var message T
		for delivery := range deliveries {
			err = json.Unmarshal(delivery.Body, &message)
			if err != nil {
				log.Printf("could not store the JSON data: %v", err)
				continue
			}

			handle := handler(message)
			switch handle {
			case Ack:
				err = delivery.Ack(false)
				if err != nil {
					log.Printf("could not acknowledge message: %v", err)
				}
			case NackRequeue:
				err = delivery.Nack(false, true)
				log.Print("message was requeued")
				if err != nil {
					log.Printf("could not requeue message: %v", err)
				}
			case NackDiscard:
				err = delivery.Nack(false, false)
				if err != nil {
					log.Printf("could not discard message: %v", err)
				}
			}

		}
	}()
	return nil
}
