package main

import (
	zmq "github.com/pebbe/zmq3"

	"fmt"
	"math/rand"
	"os"
	"time"
)

func BindPublish(self string) (statebe *zmq.Socket) {
	//  Bind state backend to endpoint
	statebe, _ = zmq.NewSocket(zmq.PUB)
	statebe.Bind("ipc://" + self + "-state.ipc")
	return
}

func Subscribe(peers []string) (statefe *zmq.Socket) {
	//  Connect statefe to all peers
	statefe, _ = zmq.NewSocket(zmq.SUB)
	statefe.SetSubscribe("")
	for _, peer := range peers {
		fmt.Printf("I: connecting to state backend at '%s'\n", peer)
		statefe.Connect("ipc://" + peer + "-state.ipc")
	}
	return
}

func ProcessEvents(self string, statebe, statefe *zmq.Socket) {
	poller := zmq.NewPoller()
	poller.Add(statefe, zmq.POLLIN)
	for {
		//  Poll for activity, or 1 second timeout
		sockets, err := poller.Poll(time.Second)
		if err != nil {
			break
		}
		//  Handle incoming status messages
		if len(sockets) == 1 {
			msg, _ := statefe.RecvMessage(0)
			peerName := msg[0]
			available := msg[1]
			fmt.Printf("%s - %s workers free\n", peerName, available)
		} else {
			statebe.SendMessage(self, rand.Intn(10))
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("syntax: peering1 me {you}...")
		os.Exit(1)
	}
	self := os.Args[1]
	fmt.Printf("I: preparing broker at %s...\n", self)
	rand.Seed(time.Now().UnixNano())
	statebe := BindPublish(self)
	defer statebe.Close()
	statefe := Subscribe(os.Args[2:])
	defer statefe.Close()
	ProcessEvents(self, statebe, statefe)
}
