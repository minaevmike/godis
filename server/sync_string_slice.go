package server

import "sync"

type syncStringSlice struct {
	data []string
	mu   sync.Mutex
}

func (s *syncStringSlice) add(key string) {
	s.mu.Lock()
	s.data = append(s.data, key)
	s.mu.Unlock()
}
