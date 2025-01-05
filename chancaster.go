package chancaster

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

const (
	// ChanNotFound error message returned when a channel is not found for a key
	ChanNotFound = "no channel found for key"
)

// ChanFunc defines the function signature expected by ChanCasters.Add().
// This function should not take any arguments and must return an error if there is one.
type ChanFunc func() error

// ChanCaster is a generic struct that manages concurrent channels associated with keys.
// It uses an errgroup to manage the execution of functions associated with each channel.
// K: The type of key used to access channels.
// T: The type of data that will be sent and received on the channels.
type ChanCaster[K comparable, T any] struct {
	eg    *errgroup.Group
	ctx   context.Context
	chans map[K]chan T
}

// New creates a new ChanCasters instance with the provided context.
// The context will be used to cancel the execution of functions within the errgroup.
// The returned errgroup context will be canceled the first time a function
// returns a non-nil error or the first time Wait returns, whichever occurs first.
func New[K comparable, T any](ctx context.Context) *ChanCaster[K, T] {
	group, ctx := errgroup.WithContext(ctx)
	return &ChanCaster[K, T]{
		eg:    group,
		ctx:   ctx,
		chans: make(map[K]chan T),
	}
}

// Add creates a new channel associated with the provided key and runs the provided function concurrently.
// The function should not take any arguments and must return an error if there is one.
// If the context is already canceled, an error is returned.
func (c *ChanCaster[K, T]) Add(key K, fn ChanFunc) error {
	if c.ctx.Err() != nil {
		return context.Cause(c.ctx)
	}

	c.chans[key] = make(chan T)
	c.eg.Go(fn)
	return nil
}

// Get retrieves the channel associated with the provided key.
// If the context is already canceled, an error is returned.
// If the key is not found, a nil channel and CHAN_NOT_FOUND error are returned.
func (c *ChanCaster[K, T]) Get(key K) (chan T, error) {
	if c.ctx.Err() != nil {
		return nil, context.Cause(c.ctx)
	}

	return c.chans[key], nil
}

// Publish sends the provided data to the channel associated with the given key.
//
// If no channel is found for the specified key, a ChanNotFound error is returned.
func (c *ChanCaster[K, T]) Publish(key K, data T) error {
	ch, ok := c.chans[key]
	if !ok {
		return fmt.Errorf("%s", ChanNotFound)
	}

	ch <- data
	return nil
}

// Close closes the channel associated with the provided key and removes it from the map.
// If the key is not found, a CHAN_NOT_FOUND error is returned.
func (c *ChanCaster[K, T]) Close(key K) error {
	ch, ok := c.chans[key]
	if !ok {
		return fmt.Errorf("%s", ChanNotFound)
	}

	close(ch)
	delete(c.chans, key)
	return nil
}

// Wait blocks until all functions added with Add() have finished execution.
// It returns the first non-nil error (if any) from the functions.
func (c *ChanCaster[K, T]) Wait() error {
	return c.eg.Wait()
}
