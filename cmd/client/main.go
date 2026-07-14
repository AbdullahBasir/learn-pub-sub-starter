package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/AbdullahBasir/learn-pub-sub-starter/internal/gamelogic"
	"github.com/AbdullahBasir/learn-pub-sub-starter/internal/pubsub"
	"github.com/AbdullahBasir/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	connection := "amqp://guest:guest@localhost:5672/"

	newCon, err := amqp.Dial(connection)
	if err != nil {
		log.Fatalf("could not dial and return new connection, %v", err)
	}

	defer newCon.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not to find client %v", err)
	}

	_, _, err = pubsub.DeclareAndBind(newCon, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatalf("could not declare and bind queue, %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

}
