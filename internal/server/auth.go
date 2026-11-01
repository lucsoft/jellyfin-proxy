package server

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// The proxy serves media from its own cache, so Jellyfin never sees those
// requests. To keep Jellyfin in charge of who may watch, every stream,
// segment and init request must carry a token that Jellyfin accepts.
// Results are cached so a running stream does not ask the server per segment.

const (
	tokenValidFor   = 10 * time.Minute
	tokenInvalidFor = time.Minute
)

type tokenCache struct {
	mu      sync.Mutex
	entries map[string]tokenEntry
}

type tokenEntry struct {
	ok    bool
	until time.Time
}

// remember marks a token as valid, e.g. after Jellyfin answered a
// PlaybackInfo request made with it.
func (s *Server) rememberToken(token string) {
	if token == "" {
		return
	}
	s.tokens.mu.Lock()
	defer s.tokens.mu.Unlock()
	if s.tokens.entries == nil {
		s.tokens.entries = map[string]tokenEntry{}
	}
	s.tokens.entries[token] = tokenEntry{ok: true, until: time.Now().Add(tokenValidFor)}
}

// authorized reports whether the request carries a token Jellyfin accepts.
func (s *Server) authorized(r *http.Request) bool {
	token := tokenFrom(r)
	if token == "" {
		return false
	}
	s.tokens.mu.Lock()
	e, found := s.tokens.entries[token]
	s.tokens.mu.Unlock()
	if found && time.Now().Before(e.until) {
		return e.ok
	}

	ok := s.checkToken(r.Context(), token)
	s.tokens.mu.Lock()
	if s.tokens.entries == nil {
		s.tokens.entries = map[string]tokenEntry{}
	}
	ttl := tokenInvalidFor
	if ok {
		ttl = tokenValidFor
	}
	s.tokens.entries[token] = tokenEntry{ok: ok, until: time.Now().Add(ttl)}
	s.tokens.mu.Unlock()
	return ok
}

// checkToken asks Jellyfin who the token belongs to; any 2xx means valid.
func (s *Server) checkToken(ctx context.Context, token string) bool {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.opt.Upstream.String()+"/Users/Me", nil)
	if err != nil {
		return false
	}
	req.Header.Set("X-Emby-Token", token)
	resp, err := s.opt.APIClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// requireAuth answers 401 and returns false when the request has no valid token.
func (s *Server) requireAuth(w http.ResponseWriter, r *http.Request) bool {
	if s.authorized(r) {
		return true
	}
	http.Error(w, "a valid Jellyfin token is required (api_key or Authorization header)", http.StatusUnauthorized)
	return false
}

// adoptToken gives a source without a token (e.g. taken over from the
// previous process) the already validated token of the current request, so
// the cache can download from Jellyfin.
func (s *Server) adoptToken(src *Source, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if src.token == "" {
		src.token = tokenFrom(r)
	}
}
