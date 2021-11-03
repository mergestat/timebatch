## timebatch

`timebatch` is a package for batching messages over a time interval.
This can be useful for receiving messages that occur "quickly" and sending them on a slower (constant) interval in batches.


```golang
b := timebatch.New(3 * time.Second) // creates a new batcher

// register a receiver to listen for batches of messages
go func() {
	for _, b := range batches {
		fmt.Println(b) // this is a batch of messages
	}
}()

// register a producer to send messages
go func() {
	b.Send(1)
	b.Send(2)
	b.Send(3)
    b.Close() // must be closed once all messages are sent to clean up
}()

// output will look something like: [1, 2, 3]
// as there were only 3 messages sent within the first time interval
```
