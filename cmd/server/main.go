package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	err = pubsub.PublishJSON(con, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Program is shutting down")
}
