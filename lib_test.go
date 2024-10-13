package chanretype

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewTransformChannel_Success(t *testing.T) {
	input := make(chan int, 3)
	output, _ := NewTransformChannel(context.Background(), input, func(ctx context.Context, i int) (string, error) {
		return fmt.Sprintf("number: %d", i), nil
	}, 2)

	go func() {
		for i := 0; i < 5; i++ {
			input <- i
		}
		close(input)
	}()

	var results []string
	for result := range output {
		results = append(results, result)
	}

	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}
}

func TestNewTransformChannel_Error(t *testing.T) {
	input := make(chan int, 5)
	_, errChan := NewTransformChannel(context.Background(), input, func(ctx context.Context, i int) (string, error) {
		return "", errors.New("transform error")
	}, 2)

	go func() {
		for i := 0; i < 5; i++ {
			input <- i
		}
		close(input)
	}()

	select {
	case err := <-errChan:
		if err == nil || err.Error() != "transform error" {
			t.Errorf("Expected 'transform error', got '%v'", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Expected error, but none received")
	}
}

func TestNewTransformChannel_ErrorOnThirdEvent(t *testing.T) {
	input := make(chan int, 5)
	output, errChan := NewTransformChannel(context.Background(), input, func(ctx context.Context, i int) (string, error) {
		if i <= 2 {
			time.Sleep(time.Duration(i) * 20 * time.Millisecond)
		}
		if i == 2 { // Return an error on the third element (index 2)
			return "", errors.New("transform error on third event")
		}
		// The latest events will be processed faster than events with index <= 2
		// in this case all subsequent events will release the locks earlier than the previous ones
		// this case cover the case <-stopPipeProcessingCh in transform gorutine
		return fmt.Sprintf("number: %d", i), nil
	}, 3)

	go func() {
		for i := 0; i < 5; i++ {
			input <- i
		}
		close(input)
	}()

	var results []string
	var errReceived error

	// Use WaitGroup to wait for results processing to complete
	var wg sync.WaitGroup
	wg.Add(1)

	// Wait for results
	go func() {
		defer wg.Done()
		for result := range output {
			results = append(results, result)
		}
	}()

	// Wait for processing to complete
	time.Sleep(250 * time.Millisecond) // Give it some time to complete

	// Wait until all results are processed
	wg.Wait()

	// Check that an error occurred
	select {
	case errReceived = <-errChan:
		if errReceived == nil || errReceived.Error() != "transform error on third event" {
			t.Errorf("Expected 'transform error on third event', got '%v'", errReceived)
		}
	default:
		t.Error("Expected an error due to transformation failure, but none received")
	}

	// Check that the first two results were processed successfully
	if len(results) != 2 {
		t.Errorf("Expected 2 results before error, got %d, results: %v", len(results), results)
	} else {
		t.Logf("Processed results: %v", results)
	}
}

func TestNewTransformChannel_CancelContext(t *testing.T) {
	input := make(chan int, 5)
	ctx, cancel := context.WithCancel(context.Background())
	_, errChan := NewTransformChannel(ctx, input, func(ctx context.Context, i int) (string, error) {
		time.Sleep(100 * time.Millisecond) // Simulate long processing
		return fmt.Sprintf("number: %d", i), nil
	}, 2)

	go func() {
		for i := 0; i < 5; i++ {
			input <- i
		}
		close(input)
	}()

	cancel() // Cancel the context

	select {
	case err := <-errChan:
		if err == nil || err.Error() != context.Canceled.Error() {
			t.Errorf("Expected context canceled error, got '%v'", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Expected error, but none received")
	}
}

func TestNewTransformChannel_EmptyInput(t *testing.T) {
	input := make(chan int)
	output, errChan := NewTransformChannel(context.Background(), input, func(ctx context.Context, i int) (string, error) {
		return "", nil
	}, 2)

	close(input) // Immediately close the input channel

	results := []string{}
	for result := range output {
		results = append(results, result)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}

	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}
	default:
		// There should be no errors
	}
}

func TestNewTransformChannel_TimeoutContext(t *testing.T) {
	input := make(chan int, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel() // Cancel the context after the test completes

	output, errChan := NewTransformChannel(ctx, input, func(ctx context.Context, i int) (string, error) {
		// Simulate long processing
		time.Sleep(100 * time.Millisecond)
		return fmt.Sprintf("number: %d", i), nil
	}, 2)

	go func() {
		for i := 0; i < 5; i++ {
			input <- i
		}
		close(input)
	}()

	var results []string
	var errReceived error

	// Use WaitGroup to wait for results processing to complete
	var wg sync.WaitGroup
	wg.Add(1)

	// Wait for results within the timeout
	go func() {
		defer wg.Done()
		for result := range output {
			results = append(results, result)
		}
	}()

	// Wait for processing to complete
	time.Sleep(250 * time.Millisecond) // Give it some time to complete

	// Wait until all results are processed
	wg.Wait()

	// Check that the timeout occurred
	select {
	case errReceived = <-errChan:
		if errReceived == nil || errReceived.Error() != context.DeadlineExceeded.Error() {
			t.Errorf("Expected context deadline exceeded error, got '%v'", errReceived)
		}
	default:
		t.Error("Expected an error due to context timeout, but none received")
	}

	// Check that some events were processed
	if len(results) > 0 && len(results) < 4 {
		t.Logf("Processed results: %v", results)
	} else {
		t.Error("Expected some results to be processed")
	}
}

func TestNewTransformChannel_ConcurrencyPreservesOrder(t *testing.T) {
	input := make(chan int, 100)
	concurrencyLevels := []int{1, 2, 4, 8} // Different levels of concurrency

	for _, concurrency := range concurrencyLevels {
		// Fill the input channel
		for i := 0; i < 100; i++ {
			input <- i
		}
		close(input) // Close the channel

		// Create a slice to store results
		results := make([]string, 0, 100)

		// Start the timer
		start := time.Now()

		// Create the transformation channel
		output, errChan := NewTransformChannel(context.Background(), input, func(ctx context.Context, i int) (string, error) {
			time.Sleep(10 * time.Millisecond) // Simulate long processing
			return fmt.Sprintf("number: %d", i), nil
		}, concurrency)

		// Use WaitGroup to wait for results processing to complete
		var wg sync.WaitGroup
		wg.Add(1)

		// Read results
		go func() {
			defer wg.Done()
			for result := range output {
				results = append(results, result)
			}
		}()

		// Wait for completion
		err := <-errChan
		if err != nil {
			t.Fatalf("Error during processing: %v", err)
		}

		// Wait until all results are processed
		wg.Wait()

		// Check that all elements in the output channel are in the correct order
		if len(results) != 100 {
			t.Errorf("Expected 100 results, got %d", len(results))
		} else {
			for i := 0; i < 100; i++ {
				expected := fmt.Sprintf("number: %d", i)
				if results[i] != expected {
					t.Errorf("Expected result at index %d to be '%s', got '%s'", i, expected, results[i])
				}
			}
		}

		// Log processing time
		duration := time.Since(start)
		t.Logf("Concurrency %d took %v", concurrency, duration)

		// For the next test, we need to reopen the channel
		input = make(chan int, 100)
	}
}

func TestNewTransformChannel_SlowInputWithError(t *testing.T) {
	input := make(chan int, 5)
	output, errChan := NewTransformChannel(context.Background(), input, func(ctx context.Context, i int) (string, error) {
		time.Sleep(20 * time.Millisecond)
		if i == 2 { // Return an error on the third element (index 2)
			return "", errors.New("transform error on third element")
		}
		return fmt.Sprintf("number: %d", i), nil
	}, 2) // Set concurrency to 2

	var wg sync.WaitGroup
	wg.Add(1)

	var wgread sync.WaitGroup
	wgread.Add(1)

	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			time.Sleep(100 * time.Millisecond) // Simulate slow input
			input <- i
		}
		close(input)
	}()

	var results []string
	var errReceived error

	// Wait for results
	go func() {
		defer wgread.Done()
		for result := range output {
			results = append(results, result)
		}
	}()

	// Allow some time for processing
	time.Sleep(500 * time.Millisecond)
	// Wait for goroutines to finish
	wg.Wait()
	wgread.Wait()

	// Check for errors
	select {
	case errReceived = <-errChan:
		if errReceived == nil || errReceived.Error() != "transform error on third element" {
			t.Errorf("Expected 'transform error on third element', got '%v'", errReceived)
		}
	default:
		t.Error("Expected an error due to transformation failure, but none received")
	}

	// Check that the first two results were processed successfully
	if len(results) != 2 {
		t.Errorf("Expected 2 results before error, got %d, results: %v", len(results), results)
	} else {
		t.Logf("Processed results: %v", results)
	}
}

func TestNewTransformChannel_ZeroConcurrency(t *testing.T) {
	input := make(chan int)
	output, errChan := NewTransformChannel(context.Background(), input, func(ctx context.Context, i int) (string, error) {
		time.Sleep(20 * time.Millisecond)
		return fmt.Sprintf("number: %d", i), nil
	}, 0) // Setting concurrency to zero

	input <- 1

	result := <-output

	if result != "number: 1" {
		t.Errorf("Expected 'number: 1', got '%s'", result)
	}

	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("Expected no error, got '%v'", err)
		}
	default:
		// No error
	}
}

func TestNewTransformChannel_VaryingConcurrencyLevels(t *testing.T) {

	concurrencyLevels := []int{1, 5, 10}

	eventCount := 10 // must be greater than or equal the level of concurrency

	for _, concurrency := range concurrencyLevels {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			var activeCount int32
			var maxActiveCount int32 // To track the maximum number of active goroutines
			var mu sync.Mutex
			input := make(chan int, 10)
			output, errChan := NewTransformChannel(context.Background(), input, func(ctx context.Context, i int) (string, error) {
				// Increment the active count and check the result
				currentCount := atomic.AddInt32(&activeCount, 1)

				// Update the maximum active count safely
				mu.Lock()
				if currentCount > maxActiveCount {
					maxActiveCount = currentCount
				}
				mu.Unlock()

				// Check that the active count does not exceed the concurrency level
				if currentCount > int32(concurrency) {
					return "", fmt.Errorf("active goroutines exceeded concurrency level: got %d, expected max %d", currentCount, concurrency)
				}

				defer atomic.AddInt32(&activeCount, -1) // Decrement when done

				time.Sleep(50 * time.Millisecond) // Simulate processing time
				return fmt.Sprintf("number: %d", i), nil
			}, concurrency)

			// Feed the input channel
			go func() {
				for i := 0; i < eventCount; i++ {
					input <- i
				}
				close(input)
			}()

			var results []string
			var receivedError error

			// Use WaitGroup to wait for results processing to complete
			var wg sync.WaitGroup
			wg.Add(1)

			// Collect results
			go func() {
				defer wg.Done()
				for result := range output {
					results = append(results, result)
				}
			}()

			wg.Add(1)
			// Check for errors
			go func() {
				defer wg.Done()
				for err := range errChan {
					receivedError = err
				}
			}()

			// Wait until all results are processed
			wg.Wait()

			// Check if an error was received indicating concurrency exceeded
			if receivedError != nil {
				t.Error(receivedError)
			}

			// Ensure all results are processed
			if len(results) != eventCount {
				t.Errorf("Expected 10 results, got %d", len(results))
			}

			// Check if the maximum active count reached the concurrency level
			if maxActiveCount < int32(concurrency) {
				t.Errorf("Expected maximum active count to reach %d, got %d", concurrency, maxActiveCount)
			}
		})
	}
}

func ExampleNewTransformChannel() {
	// Create the input channel
	input := make(chan int)

	// Create a context
	ctx := context.Background()

	// Create the transformation channel
	output, errChan := NewTransformChannel(ctx, input, func(ctx context.Context, i int) (string, error) {

		dur := time.Duration(rand.Intn(1000)) * time.Millisecond
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(dur):

		}

		return fmt.Sprintf("number: %d", i), nil
	}, 3)

	// Start a goroutine to send data to the input channel
	go func() {
		for i := 0; i < 5; i++ {
			input <- i
		}
		close(input) // Close the input channel
	}()

	// Read from the output channel and print the results
	for result := range output {
		fmt.Println(result)
	}

	// Check for errors
	select {
	case err := <-errChan:
		if err != nil {
			fmt.Println("Error:", err)
		}
	default:
		// No errors
	}

	// Output:
	// number: 0
	// number: 1
	// number: 2
	// number: 3
	// number: 4
}
