package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
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

			handler(message)

			err = delivery.Ack(false)
			if err != nil {
				log.Printf("could not deliver message: %v", err)
			}
		}
	}()

	return nil
}
