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
		}
		log.Print("invalid command")
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Program is paused")
}
