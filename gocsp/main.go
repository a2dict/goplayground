package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/a2dict/goplayground/gocsp/chain"
	fuzz "github.com/google/gofuzz"
)

func main() {
	// for graceful shutdown
	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, os.Interrupt, os.Kill, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	msgC := make(chan *chain.InComingMessage)
	taskC := chain.PersistInComingMessage(ctx, msgC)
	resC := chain.HandleMessageTask(ctx, taskC)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	f := fuzz.New()
	// generate message
	go func() {
		for {
			time.Sleep(time.Duration(r.Intn(8)) * time.Second)
			msg := &chain.InComingMessage{}
			f.Fuzz(msg)
			msgC <- msg
		}
	}()

	// handling task_result
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Printf("stop handling task_result")
				return
			case res := <-resC:
				log.Printf("handling task_result:%v", res)
			}

		}
	}()

	<-signalC
	cancel()

	time.Sleep(3 * time.Second)
	log.Fatal("exit")

}
