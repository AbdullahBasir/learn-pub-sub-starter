package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"

	"github.com/AbdullahBasir/learn-pub-sub-starter/internal/gamelogic"
	"github.com/AbdullahBasir/learn-pub-sub-starter/internal/pubsub"
	"github.com/AbdullahBasir/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connection := "amqp://guest:guest@localhost:5672/"

	newCon, err := amqp.Dial(connection)
	if err != nil {
		log.Fatal(err)
	}

	defer newCon.Close()

	fmt.Println("Connection is Successful")

	con, err := newCon.Channel()
	if err != nil {
		log.Fatal(err)
	}

	err = pubsub.SubscribeGob(newCon, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.Durable, handlerGameLog, func(data []byte) (routing.GameLog, error) {
		decoder := gob.NewDecoder(bytes.NewReader(data))
		var message routing.GameLog
		err := decoder.Decode(&message)
		return message, err
	})
	if err != nil {
		log.Fatalf("could not consume logs from gamelog: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if words == nil {
			continue
		}
		if words[0] == "pause" {
			log.Print("sending a pause message")
			err = pubsub.PublishJSON(con, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				log.Fatal(err)
			}
		} else if words[0] == "resume" {
			log.Print("sending a resume message")
			err = pubsub.PublishJSON(con, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				log.Fatal(err)
			}
		} else if words[0] == "quit" {
			log.Print("exiting")
			break
		} else {
			log.Print("invalid command")
		}
	}
}
