package main

import (
	"fmt"
	"log"

	"github.com/AbdullahBasir/learn-pub-sub-starter/internal/gamelogic"
	"github.com/AbdullahBasir/learn-pub-sub-starter/internal/pubsub"
	"github.com/AbdullahBasir/learn-pub-sub-starter/internal/routing"
)

func handlerGameLog(logEntry routing.GameLog) pubsub.AckType {
	defer fmt.Print("> ")
	err := gamelogic.WriteLog(logEntry)
	if err != nil {
		log.Printf("could not write game log: %v", err)
		return pubsub.NackRequeue
	}
	log.Print("game log was acknowledged")
	return pubsub.Ack
}
