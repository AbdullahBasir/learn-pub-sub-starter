package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Program is shutting down")
}
