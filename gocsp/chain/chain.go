package chain

import (
	"context"
	"log"
	"time"
)

type InComingMessage struct {
	MsgID   string
	Content string
	Created time.Time
}

type MessageTask struct {
	TaskID   string
	Content  string
	Callback func(stat string)
}

type TaskResult struct {
	TaskID string
	Stat   string
}

func PersistInComingMessage(ctx context.Context, in <-chan *InComingMessage) <-chan *MessageTask {
	ret := make(chan *MessageTask)
	threads := 4
	for i := 0; i < threads; i++ {
		go func(idx int) {
			log.Printf("start persisting, id:%v\n", idx)
			for {
				select {
				case <-ctx.Done():
					log.Printf("stop persisting thread, id:%v\n", idx)
					return
				case msg := <-in:
					// persisting
					log.Printf("persist msg:%v\n", msg)

					// new MessageTask
					t := &MessageTask{
						TaskID:  msg.MsgID,
						Content: msg.Content,
						Callback: func(stat string) {
							log.Println("task callback..")
						},
					}
					ret <- t
				}

			}
		}(i)
	}
	return ret
}

func HandleMessageTask(ctx context.Context, in <-chan *MessageTask) <-chan *TaskResult {
	ret := make(chan *TaskResult)
	threads := 5
	for i := 0; i < threads; i++ {
		go func(idx int) {
			log.Printf("start task handler, id:%v\n", idx)
			for {
				select {
				case <-ctx.Done():
					log.Printf("stop task handler, id:%v\n", idx)
					return
				case t := <-in:
					// handle message task
					log.Printf("handle message task, task:%v\n", t)

					tr := &TaskResult{
						TaskID: t.TaskID,
						Stat:   "success",
					}
					ret <- tr
				}
			}

		}(i)
	}

	return ret
}
