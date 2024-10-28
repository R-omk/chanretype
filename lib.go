// Package chanretype provides functionality for transforming
// data from one channel to another using a specified transformation function
// and parallel processing.
package chanretype

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// TransformFunc defines a transformation function that takes
// a context and an input value, and returns a transformed value
// and an error, if one occurred.
type TransformFunc[I any, O any] func(context.Context, I) (O, error)

// NewTransformChannel creates a new channel for transforming data from
// an input channel to an output channel using the specified transformation function.
// The function supports concurrent processing, with the number of goroutines
// specified by the concurrency parameter.
//
// This function preserves the order of events regardless of the order of
// completion of transformations. If an error occurs during the transformation,
// the output channel will be closed only after all previously processed events
// have been written to the channel.
//
// Parameters:
//   - ctx: the context that can be used for cancellation
//     of processing and deadline propagation.
//   - inputChan: the channel from which input data will be read.
//   - transform: a function that will be applied to each element
//     from the input channel. It should return a transformed value
//     and an error, if one exists.
//   - concurrency: the level of concurrency that determines the number
//     of goroutines executing simultaneously to process the input data.
//     If the value is less than or equal to zero, a value of 1 will be used.
//
// Return values:
//   - outputChan: a channel from which transformed
//     data can be read. The capacity of the channel is equal to the capacity of the input channel.
//   - errorChan: a channel that will contain errors that occurred
//     during data transformation. The channel is closed without error events
//     when the input channel is also closed.
//
// Example usage:
//
//	ctx := context.Background()
//	inputChan := make(chan int)
//	outputChan, errorChan := NewTransformChannel(ctx, inputChan, func(ctx context.Context, i int) (string, error) {
//	    return fmt.Sprintf("number: %d", i), nil
//	}, 2)
//
//	go func() {
//	    for i := 0; i < 5; i++ {
//	        inputChan <- i
//	    }
//	    close(inputChan)
//	}()
//
//	for output := range outputChan {
//	    fmt.Println(output)
//	}
//
//	if err := <-errorChan; err != nil {
//	    fmt.Println("Error:", err)
//	}
//
// This function is intended for use in situations
// where data needs to be processed from a channel using
// parallel processing and transformation.
func NewTransformChannel[I any, O any](
	ctx context.Context,
	inputChan <-chan I,
	transform TransformFunc[I, O],
	concurrency int,
) (chan O, chan error) {

	if concurrency <= 0 {
		concurrency = 1
	}

	outputChan := make(chan O, cap(inputChan))
	resultErrorChan := make(chan error, 1)
	var mu sync.Mutex
	var resultError error

	// run pipe processing
	go func() {

		var pipeWg sync.WaitGroup

		defer close(outputChan)
		defer close(resultErrorChan)

		stopPipeCh := make(chan struct{})

		stopPipe, pipeIsStopped := createSafeCloser(stopPipeCh)

		defer func() {
			stopPipe()
		}()

		terminateWithError := func(err error) {
			mu.Lock()
			if resultError == nil {

				// store only first error and stop pipe only once
				resultError = err
				stopPipe()
			}
			mu.Unlock()
		}

		// run context done processing
		stopedDoneProcessingCh := make(chan struct{})
		defer close(stopedDoneProcessingCh)
		go func() {
			select {
			case <-stopedDoneProcessingCh:
				return
			case <-ctx.Done():
				terminateWithError(ctx.Err())
			}
			return
		}()

		prevEventLockCh := make(chan struct{})
		close(prevEventLockCh) // the first event is not blocked by anyone, unlock instantly

		sem := make(chan struct{}, concurrency)
		defer close(sem)

		func() {
			var input I
			for {

				select {
				case <-stopPipeCh:
					return

				case sem <- struct{}{}:
					select {
					case <-stopPipeCh:
						return
					case event, ok := <-inputChan:

						if !ok {
							return
						}
						input = event
					}
				}

				eventLockCh := make(chan struct{})

				pipeWg.Add(1)
				go func(prevLockCh <-chan struct{}, lockCh chan<- struct{}, input I) {

					transformed, err := transform(ctx, input)

					select {
					case <-stopPipeCh:
						time.Sleep(0) // dummy func for coverage detect
					// safe to unlock and free goroutine because pipe is stopped
					case <-prevLockCh: // waiting for the previous event to be written to output channel

					}

					if !pipeIsStopped() {
						if err != nil {
							terminateWithError(err)
						} else {
							outputChan <- transformed
						}
					} // else: nothing to do with results, first error already stored and pipe is terminated

					close(lockCh) // unlock next event
					<-sem
					pipeWg.Done()

				}(prevEventLockCh, eventLockCh, input)

				prevEventLockCh = eventLockCh
			}
		}()

		pipeWg.Wait()

		mu.Lock()
		if resultError != nil {
			resultErrorChan <- resultError
		}
		mu.Unlock()

	}()

	return outputChan, resultErrorChan
}

// createSafeCloser returns a function to safely close the channel
// and another function to check if the channel is closed.
func createSafeCloser[T any](ch chan T) (closeFunc func(), isClosedFunc func() bool) {
	var closed uint32

	closeFunc = func() {
		if atomic.CompareAndSwapUint32(&closed, 0, 1) {
			close(ch)
		}
	}

	isClosedFunc = func() bool {
		return atomic.LoadUint32(&closed) == 1
	}

	return closeFunc, isClosedFunc
}
