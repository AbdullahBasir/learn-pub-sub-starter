package main

import (
	"fmt"
	"log"

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

	con, err := newCon.Channel()
	if err != nil {
		log.Fatal(err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not to find client %v", err)
	}

	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(newCon, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, routing.ArmyMovesPrefix+".*", pubsub.Transient, handlerMove(gameState, con))
	if err != nil {
		log.Fatalf("could not declare and bind queue, %v", err)
	}

	err = pubsub.SubscribeJSON(newCon, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		log.Fatalf("could not declare and bind queue, %v", err)
	}

	err = pubsub.SubscribeJSON(newCon, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, routing.WarRecognitionsPrefix+".*", pubsub.Durable, handlerWar(gameState))
	if err != nil {
		log.Fatalf("could not declare and bind queue, %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) < 1 {
			continue
		}

		if words[0] == "spawn" {
			err = gameState.CommandSpawn(words)
			if err != nil {
				continue
			}

		} else if words[0] == "move" {
			armyMove, err := gameState.CommandMove(words)
			if err != nil {
				continue
			}
			err = pubsub.PublishJSON(con, routing.ExchangePerilTopic, "army_moves."+username, armyMove)
			if err != nil {
				log.Fatal(err)
			}
			log.Print("The move was successfully made!\n")

		} else if words[0] == "status" {
			gameState.CommandStatus()

		} else if words[0] == "help" {
			gamelogic.PrintClientHelp()

		} else if words[0] == "spam" {
			fmt.Print("Spamming not allowed yet!\n")

		} else if words[0] == "quit" {
			gamelogic.PrintQuit()
			break

		} else {
			fmt.Print("error, command not found\n")
			continue
		}
	}
}
