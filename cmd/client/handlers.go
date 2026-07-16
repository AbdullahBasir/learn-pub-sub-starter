package main

import (
	"fmt"
	"log"

	"github.com/AbdullahBasir/learn-pub-sub-starter/internal/gamelogic"
	"github.com/AbdullahBasir/learn-pub-sub-starter/internal/pubsub"
	"github.com/AbdullahBasir/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		log.Print("message was acknowledged")
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)

		switch outcome {
		case gamelogic.MoveOutcomeMakeWar:
			log.Print("message was acknowledged")
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+gs.GetUsername(), gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			},
			)
			if err != nil {
				log.Printf("could not publish message: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		case gamelogic.MoveOutComeSafe:
			log.Print("message was acknowledged")
			return pubsub.Ack
		default:
			log.Print("message was discarded")
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(war gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, _, _ := gs.HandleWar(war)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			log.Print("requeued the message")
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			log.Print("discarded the message")
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			log.Print("acknowledged the message")
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			log.Print("acknowledged the message")
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			log.Print("acknowledged the message")
			return pubsub.Ack
		default:
			log.Print("discarded the message")
			return pubsub.NackDiscard
		}
	}
}
