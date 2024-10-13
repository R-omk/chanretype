## Overview

This package enables you to create a transformation channel that reads from an input channel, applies a transformation function to each element, and writes the results to an output channel. The processing is concurrent, allowing multiple goroutines to handle the transformation simultaneously. The order of events is preserved regardless of the completion order of transformations.

## Features

- **Concurrent Processing:** Specify the number of goroutines for processing.
- **Error Handling:** Errors during transformation are captured and sent to a dedicated error channel.
- **Order Preservation:** The order of events is maintained in the output channel.
- **Graceful Shutdown:** The output channel closes only after all processed events before an error are written.

## Example usage

```golang
package main

import (
	"context"
	"fmt"

	chanretype "github.com/R-omk/chanretype"
)

func main() {

	inputChan := make(chan int)
	concurrency := 2
	outputChan, errorChan := chanretype.NewTransformChannel(
		context.Background(),
		inputChan,
		func(ctx context.Context, i int) (string, error) {
			return fmt.Sprintf("number: %d", i), nil
		},
		concurrency,
	)

	go func() {
		for i := 0; i < 5; i++ {
			inputChan <- i
		}
		close(inputChan)
	}()

	for output := range outputChan {
		fmt.Println(output)
	}

	if err := <-errorChan; err != nil {
		fmt.Println("Error:", err)
	}
}


```