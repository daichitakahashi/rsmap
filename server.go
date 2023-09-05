package rsmap

// type (
// 	serverCore struct {
// 		db        *bbolt.DB
// 		resources map[string]*resource
// 	}
// 	resource struct {
// 		init     chan bool
// 		sem      *semaphore.Weighted
// 		logs     logs
// 		channels map[string]<-chan bool
// 	}
// )

// func (c *serverCore) startInit(ctx context.Context, resourceName string, max int64) (initStatus, error) {
// 	var (
// 		r      *resource
// 		status initStatus
// 	)
// 	err := c.db.Update(func(tx *bbolt.Tx) error {
// 		var ok bool
// 		r, ok = c.resources[resourceName]
// 		if !ok {
// 			r = &resource{
// 				init:     make(chan bool),
// 				sem:      semaphore.NewWeighted(max),
// 				channels: map[string]<-chan bool{},
// 			}
// 			c.resources[resourceName] = r
// 		}
// 		status = r.logs.tryInit(tx, time.Now(), max)

// 		return nil
// 	})
// 	if err != nil {
// 		return "", err
// 	}

// 	switch status {
// 	case statusProcessing:
// 		select {
// 		case <-ctx.Done():
// 			return "", ctx.Err()
// 		case <-r.init:
// 			st, _ := r.logs.initResult()
// 			return st, nil
// 		}
// 	default:
// 		return status, nil
// 	}
// }

// func (c *serverCore) completeInit(ctx context.Context, resourceName string, data any) error {
// 	err := c.db.Update(func(tx *bbolt.Tx) error {
// 		r, ok := c.resources[resourceName]
// 		if !ok {
// 			return errors.New("resource is not initialized")
// 		}
// 		err := r.logs.completeInit(tx, data, time.Now())
// 		if err != nil {
// 			return err
// 		}
// 		close(r.init)
// 		return nil
// 	})
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (c *serverCore) failInit(ctx context.Context, resourceName string) error {
// 	err := c.db.Update(func(tx *bbolt.Tx) error {
// 		r, ok := c.resources[resourceName]
// 		if !ok {
// 			return errors.New("resource is not initialized")
// 		}
// 		err := r.logs.failInit(tx, time.Now())
// 		if err != nil {
// 			return err
// 		}
// 		close(r.init)
// 		return nil
// 	})
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (c *serverCore) acquire(ctx context.Context, resourceName, acquisitionID string, exclusive bool, v any) error {
// 	var st acquireStatus
// 	var acquired <-chan bool

// 	err := c.db.Update(func(tx *bbolt.Tx) error {
// 		r, ok := c.resources[resourceName]
// 		if !ok {
// 			return errors.New("resource is not initialized")
// 		}

// 		weight := int64(1)
// 		if exclusive {
// 			weight = r.logs.info.max
// 		}

// 		id := uuid.NewString()
// 		var err error
// 		st, err = r.logs.acquire(tx, id, weight, time.Now())
// 		if err != nil {
// 			return err
// 		}
// 		switch st {
// 		case statusRequested:
// 			ch := make(chan bool)
// 			r.channels[acquisitionID] = ch
// 			acquired = ch

// 			// perform acquire
// 			go func() {
// 				_ = r.sem.Acquire(context.Background(), weight)
// 				_ = c.db.Update(func(tx *bbolt.Tx) error {
// 					_ = r.logs.completeAcquire(tx, acquisitionID, time.Now())
// 					close(ch)
// 					return nil
// 				})
// 				delete(r.channels, acquisitionID)
// 			}()
// 		case statusAcquiring:
// 			acquired = r.channels[acquisitionID]

// 		case statusAcquired:
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	if st == statusAcquired {
// 		return nil // already acquired
// 	}

// 	// wait for acquire
// 	select {
// 	case <-ctx.Done():
// 		return ctx.Err()
// 	case <-acquired:
// 		return nil
// 	}
// }

// func (c *serverCore) useExclusive(ctx context.Context, resourceName string, acquisitionID string, v any) error {
// 	return c.acquire(ctx, resourceName, acquisitionID, true, v)
// }

// func (c *serverCore) useShared(ctx context.Context, resourceName string, acquisitionID string, v any) error {
// 	return c.acquire(ctx, resourceName, acquisitionID, false, v)
// }

// func (c *serverCore) release(ctx context.Context, resourceName string, acquisitionID string) error {
// 	err := c.db.Update(func(tx *bbolt.Tx) error {
// 		r, ok := c.resources[resourceName]
// 		if !ok {
// 			return errors.New("resource is not initialized")
// 		}
// 		return r.logs.release(tx, acquisitionID)
// 	})
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// var _ core = (*serverCore)(nil)
