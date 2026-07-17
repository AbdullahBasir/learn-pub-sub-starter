package pubsub

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
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
		for delivery := range deliveries {
			message, err := unmarshaller(delivery.Body)
			if err != nil {
				log.Printf("could not decode the data: %v", err)
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
