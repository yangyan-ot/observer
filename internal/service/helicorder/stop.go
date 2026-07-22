package helicorder

import (
	"errors"
	"fmt"
	"time"
)

func (s *HelicorderServiceImpl) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.status.SetStoppedAt(s.timeSource.Now())
	s.status.SetIsRunning(false)
	s.cancelFn()

	done := make(chan error, 1)
	go func() {
		s.wg.Wait()
		if s.dataProvider.queryCache != nil {
			if err := s.dataProvider.queryCache.Clear(); err != nil {
				done <- fmt.Errorf("failed to clear helicorder cache: %w", err)
				return
			}
		}
		done <- nil
	}()

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case err := <-done:
		return err
	case <-timer.C:
		return errors.New("timeout waiting for goroutines to finish")
	}
}
