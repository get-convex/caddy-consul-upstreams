// Package consul provides a Caddy dynamic upstream source backed by Consul's
// passing service health endpoint.
package consul

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

const (
	moduleID        = "http.reverse_proxy.upstreams.consul"
	tierPlaceholder = "http.reverse_proxy.upstreams.consul.tier"
	consulTimeout   = 5 * time.Second
)

// ConsulUpstreams discovers passing service instances through Consul.
type ConsulUpstreams struct {
	Address     string         `json:"address,omitempty"`
	Service     string         `json:"service,omitempty"`
	Refresh     caddy.Duration `json:"refresh,omitempty"`
	GracePeriod caddy.Duration `json:"grace_period,omitempty"`
	Locality    *Locality      `json:"locality,omitempty"`

	client  consulClient
	logger  *zap.Logger
	mu      sync.RWMutex
	entries map[string]cacheEntry
}

// Locality controls local-node preference.
type Locality struct {
	NodeMetaKey string `json:"node_meta,omitempty"`
	Minimum     int    `json:"minimum,omitempty"`
	localValue  string
}

type consulClient interface {
	NodeMeta() (map[string]string, error)
	Service(context.Context, string) ([]*api.ServiceEntry, error)
}

type apiClient struct{ c *api.Client }

func (a apiClient) NodeMeta() (map[string]string, error) {
	self, err := a.c.Agent().Self()
	if err != nil {
		return nil, err
	}
	config, ok := self["Config"]
	if !ok {
		return nil, errors.New("response has no Config section")
	}
	value, ok := config["NodeMeta"]
	if !ok {
		return nil, errors.New("response has no Config.NodeMeta")
	}
	meta, ok := value.(map[string]interface{})
	if !ok {
		return nil, errors.New("Config.NodeMeta is not an object")
	}
	result := make(map[string]string, len(meta))
	for key, value := range meta {
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("Config.NodeMeta[%q] is not a string", key)
		}
		result[key] = text
	}
	return result, nil
}

func (a apiClient) Service(ctx context.Context, service string) ([]*api.ServiceEntry, error) {
	entries, _, err := a.c.Health().Service(service, "", true, (&api.QueryOptions{}).WithContext(ctx))
	return entries, err
}

type cacheEntry struct {
	snapshot  snapshot
	freshness time.Time
}

type snapshot struct{ all, local []string }

func init() { caddy.RegisterModule(&ConsulUpstreams{}) }

func (*ConsulUpstreams) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: moduleID, New: func() caddy.Module { return new(ConsulUpstreams) }}
}

func (u *ConsulUpstreams) Provision(ctx caddy.Context) error {
	if err := u.Validate(); err != nil {
		return err
	}
	u.logger = ctx.Logger()
	u.entries = make(map[string]cacheEntry)
	if u.client == nil {
		client, err := newConsulClient(u.Address, consulTimeout)
		if err != nil {
			return fmt.Errorf("creating Consul client: %w", err)
		}
		u.client = apiClient{client}
	}
	if u.Locality != nil {
		meta, err := u.client.NodeMeta()
		if err != nil {
			return fmt.Errorf("reading local Consul node metadata: %w", err)
		}
		u.Locality.localValue = meta[u.Locality.NodeMetaKey]
		if u.Locality.localValue == "" {
			return fmt.Errorf("local Consul node metadata %q is missing or empty", u.Locality.NodeMetaKey)
		}
	}
	return nil
}

func newConsulClient(address string, timeout time.Duration) (*api.Client, error) {
	config := api.DefaultConfig()
	config.Address = address
	if strings.HasPrefix(address, "http://") {
		config.Scheme = "http"
	} else if strings.HasPrefix(address, "https://") {
		config.Scheme = "https"
	}
	client, err := api.NewClient(config)
	if err != nil {
		return nil, err
	}
	config.HttpClient.Timeout = timeout
	return client, nil
}

func (u *ConsulUpstreams) Validate() error {
	if u.Address == "" {
		u.Address = "http://127.0.0.1:8500"
	}
	if u.Refresh == 0 {
		u.Refresh = caddy.Duration(time.Minute)
	}
	if strings.TrimSpace(u.Service) == "" {
		return errors.New("service is required")
	}
	if u.Refresh < 0 || u.GracePeriod < 0 {
		return errors.New("refresh and grace_period must not be negative")
	}
	if u.Locality != nil {
		if strings.TrimSpace(u.Locality.NodeMetaKey) == "" {
			return errors.New("locality.node_meta is required")
		}
		if u.Locality.Minimum == 0 {
			u.Locality.Minimum = 2
		}
		if u.Locality.Minimum < 1 {
			return errors.New("locality.minimum must be at least one")
		}
	}
	return nil
}

func (u *ConsulUpstreams) GetUpstreams(r *http.Request) ([]*reverseproxy.Upstream, error) {
	service := u.expandedService(r)
	if service == "" {
		return nil, errors.New("expanded Consul service is empty")
	}

	u.mu.RLock()
	entry, ok := u.entries[service]
	u.mu.RUnlock()
	if !ok || !entry.isFresh(u.Refresh) {
		var err error
		entry, err = u.refresh(r.Context(), service)
		if err != nil {
			return nil, err
		}
	}

	tier, dials := u.selectTier(entry.snapshot)
	setTier(r, tier)
	return upstreams(dials), nil
}

func (u *ConsulUpstreams) refresh(ctx context.Context, service string) (cacheEntry, error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	entry, ok := u.entries[service]
	if ok && entry.isFresh(u.Refresh) {
		return entry, nil
	}
	entries, err := u.client.Service(ctx, service)
	if err != nil {
		if ok && u.GracePeriod > 0 {
			if u.logger != nil {
				u.logger.Warn("Consul upstream refresh failed; using cached snapshot", zap.String("service", service), zap.Error(err))
			}
			entry.freshness = time.Now().Add(time.Duration(u.GracePeriod) - time.Duration(u.Refresh))
			u.entries[service] = entry
			return entry, nil
		}
		return cacheEntry{}, fmt.Errorf("refreshing Consul service %q: %w", service, err)
	}

	if !ok && len(u.entries) >= 100 {
		var evicted string
		for key := range u.entries {
			if evicted == "" || key < evicted {
				evicted = key
			}
		}
		delete(u.entries, evicted)
	}
	entry = cacheEntry{
		snapshot:  u.makeSnapshot(entries),
		freshness: time.Now(),
	}
	u.entries[service] = entry
	return entry, nil
}

func (entry cacheEntry) isFresh(refresh caddy.Duration) bool {
	return time.Since(entry.freshness) < time.Duration(refresh)
}

func (u *ConsulUpstreams) selectTier(snapshot snapshot) (string, []string) {
	if u.Locality == nil {
		return "all", snapshot.all
	}
	if len(snapshot.local) >= u.Locality.Minimum {
		return "local", snapshot.local
	}
	return "all", snapshot.all
}

func (u *ConsulUpstreams) ResetCache(r *http.Request) error {
	if r == nil {
		u.mu.Lock()
		u.entries = make(map[string]cacheEntry)
		u.mu.Unlock()
		return nil
	}
	service := u.expandedService(r)
	u.mu.Lock()
	if entry, ok := u.entries[service]; ok {
		entry.freshness = time.Time{}
		u.entries[service] = entry
	}
	u.mu.Unlock()
	return nil
}

func (u *ConsulUpstreams) makeSnapshot(entries []*api.ServiceEntry) snapshot {
	seen := make(map[string]struct{}, len(entries))
	var result snapshot
	for _, entry := range entries {
		if entry == nil || entry.Node == nil || entry.Service == nil || entry.Service.Port < 1 || entry.Service.Port > 65535 {
			continue
		}
		host := entry.Service.Address
		if host == "" {
			host = entry.Node.Address
		}
		if host == "" {
			continue
		}
		dial := net.JoinHostPort(host, strconv.Itoa(entry.Service.Port))
		if _, ok := seen[dial]; ok {
			continue
		}
		seen[dial] = struct{}{}
		result.all = append(result.all, dial)
		if u.Locality != nil && entry.Node.Meta[u.Locality.NodeMetaKey] == u.Locality.localValue {
			result.local = append(result.local, dial)
		}
	}
	return result
}

func (u *ConsulUpstreams) expandedService(r *http.Request) string {
	if r == nil {
		return u.Service
	}
	repl, ok := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer)
	if !ok {
		return u.Service
	}
	return strings.TrimSpace(repl.ReplaceAll(u.Service, ""))
}

func upstreams(dials []string) []*reverseproxy.Upstream {
	result := make([]*reverseproxy.Upstream, len(dials))
	for index, dial := range dials {
		result[index] = &reverseproxy.Upstream{Dial: dial}
	}
	return result
}

func tier(r *http.Request) string {
	value, _ := caddyhttp.GetVar(r.Context(), tierPlaceholder).(string)
	return value
}

func setTier(r *http.Request, value string) {
	if r == nil {
		return
	}
	caddyhttp.SetVar(r.Context(), tierPlaceholder, value)
	if repl, ok := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer); ok {
		repl.Set(tierPlaceholder, value)
	}
}

func (u *ConsulUpstreams) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next()
	for d.NextBlock(0) {
		switch d.Val() {
		case "address", "service", "refresh", "grace_period":
			option := d.Val()
			if !d.NextArg() {
				return d.ArgErr()
			}
			value := d.Val()
			if d.NextArg() {
				return d.ArgErr()
			}
			switch option {
			case "address":
				u.Address = value
			case "service":
				u.Service = value
			default:
				duration, err := caddy.ParseDuration(value)
				if err != nil {
					return d.Errf("invalid %s: %v", option, err)
				}
				if option == "refresh" {
					u.Refresh = caddy.Duration(duration)
				} else {
					u.GracePeriod = caddy.Duration(duration)
				}
			}
		case "locality":
			if u.Locality != nil {
				return d.Err("locality is already specified")
			}
			u.Locality = &Locality{}
			for nesting := d.Nesting(); d.NextBlock(nesting); {
				switch d.Val() {
				case "node_meta":
					if !d.NextArg() {
						return d.ArgErr()
					}
					u.Locality.NodeMetaKey = d.Val()
					if d.NextArg() {
						return d.ArgErr()
					}
				case "minimum":
					if !d.NextArg() {
						return d.ArgErr()
					}
					minimum, err := strconv.Atoi(d.Val())
					if err != nil {
						return d.Errf("invalid locality minimum: %v", err)
					}
					u.Locality.Minimum = minimum
					if d.NextArg() {
						return d.ArgErr()
					}
				default:
					return d.Errf("unrecognized locality option %q", d.Val())
				}
			}
		default:
			return d.Errf("unrecognized consul option %q", d.Val())
		}
	}
	return u.Validate()
}

var (
	_ caddy.Module                       = (*ConsulUpstreams)(nil)
	_ caddy.Provisioner                  = (*ConsulUpstreams)(nil)
	_ caddy.Validator                    = (*ConsulUpstreams)(nil)
	_ caddyfile.Unmarshaler              = (*ConsulUpstreams)(nil)
	_ reverseproxy.UpstreamSource        = (*ConsulUpstreams)(nil)
	_ reverseproxy.CachingUpstreamSource = (*ConsulUpstreams)(nil)
)
