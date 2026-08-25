package consul

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

type fakeConsul struct {
	mu      sync.Mutex
	meta    map[string]string
	entries []*api.ServiceEntry
	err     error
	calls   int
}

func (f *fakeConsul) NodeMeta() (map[string]string, error) { return f.meta, nil }

func (f *fakeConsul) Service(context.Context, string) ([]*api.ServiceEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.entries, f.err
}

func (f *fakeConsul) set(entries []*api.ServiceEntry, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries, f.err = entries, err
}

func (f *fakeConsul) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func instance(nodeAddress, serviceAddress string, port int, meta map[string]string) *api.ServiceEntry {
	return &api.ServiceEntry{
		Node:    &api.Node{Address: nodeAddress, Meta: meta},
		Service: &api.AgentService{Address: serviceAddress, Port: port},
	}
}

func testModule(client *fakeConsul) *ConsulUpstreams {
	return &ConsulUpstreams{
		Service:     "{service}",
		Refresh:     caddy.Duration(time.Hour),
		GracePeriod: caddy.Duration(time.Second),
		client:      client,
		logger:      zap.NewNop(),
		entries:     make(map[string]cacheEntry),
	}
}

func request(service string) *http.Request {
	r, _ := http.NewRequest("GET", "http://example.test", nil)
	repl := caddy.NewReplacer()
	repl.Set("service", service)
	ctx := context.WithValue(r.Context(), caddy.ReplacerCtxKey, repl)
	ctx = context.WithValue(ctx, caddyhttp.VarsCtxKey, map[string]any{})
	return r.WithContext(ctx)
}

func TestCaddyfileDefaultsAndValidation(t *testing.T) {
	u := new(ConsulUpstreams)
	if err := u.UnmarshalCaddyfile(caddyfile.NewTestDispenser(`consul {
		service api
		locality {
			node_meta availability-zone-id
		}
	}`)); err != nil {
		t.Fatal(err)
	}
	if u.Address != "http://127.0.0.1:8500" || time.Duration(u.Refresh) != time.Minute || u.GracePeriod != 0 || u.Locality.Minimum != 2 {
		t.Fatalf("unexpected defaults: %#v", u)
	}
	if err := (&ConsulUpstreams{}).Validate(); err == nil {
		t.Fatal("missing service accepted")
	}
	if err := (&ConsulUpstreams{Service: "x", Refresh: -1}).Validate(); err == nil {
		t.Fatal("negative refresh accepted")
	}
	if err := (&ConsulUpstreams{Service: "x", Locality: &Locality{NodeMetaKey: "az", Minimum: -1}}).Validate(); err == nil {
		t.Fatal("invalid locality minimum accepted")
	}
}

func TestAPINodeMetaReadsConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/agent/self" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"Config":{"NodeMeta":{"availability-zone-id":"a"}},"Member":{"Tags":{"availability-zone-id":"wrong"}}}`))
	}))
	defer server.Close()
	config := api.DefaultConfig()
	config.Address = server.Listener.Addr().String()
	client, err := api.NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := (apiClient{client}).NodeMeta()
	if err != nil {
		t.Fatal(err)
	}
	if meta["availability-zone-id"] != "a" {
		t.Fatalf("got %#v", meta)
	}
}

func TestProvisionReadsLocalNodeMeta(t *testing.T) {
	u := &ConsulUpstreams{
		Service:  "api",
		client:   &fakeConsul{meta: map[string]string{"availability-zone-id": "a"}},
		Locality: &Locality{NodeMetaKey: "availability-zone-id"},
	}
	if err := u.Provision(caddy.Context{Context: context.Background()}); err != nil {
		t.Fatal(err)
	}
	if u.Locality.localValue != "a" {
		t.Fatalf("got local metadata %q", u.Locality.localValue)
	}
}

func TestAddressesUseServiceAddressAndKeepOrder(t *testing.T) {
	u := &ConsulUpstreams{Locality: &Locality{NodeMetaKey: "az", localValue: "a"}}
	snapshot := u.makeSnapshot([]*api.ServiceEntry{
		instance("10.0.0.1", "", 80, map[string]string{"az": "a"}),
		instance("10.0.0.2", "service.internal", 443, map[string]string{"az": "b"}),
		instance("2001:db8::1", "", 443, map[string]string{"az": "b"}),
		instance("10.0.0.1", "", 80, map[string]string{"az": "a"}),
		instance("bad", "", 0, nil),
	})
	if got, want := snapshot.all, []string{"10.0.0.1:80", "service.internal:443", "[2001:db8::1]:443"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("all: got %v, want %v", got, want)
	}
	if got, want := snapshot.local, []string{"10.0.0.1:80"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("local: got %v, want %v", got, want)
	}
}

func TestCacheRefreshResetGraceAndEmpty(t *testing.T) {
	f := &fakeConsul{entries: []*api.ServiceEntry{instance("10.0.0.1", "", 80, nil)}}
	f.set(nil, errors.New("unavailable"))
	if _, err := testModule(f).GetUpstreams(request("unavailable")); err == nil {
		t.Fatal("initial refresh error accepted")
	}
	f.set([]*api.ServiceEntry{instance("10.0.0.1", "", 80, nil)}, nil)
	u := testModule(f)
	r := request("api")
	if _, err := u.GetUpstreams(r); err != nil {
		t.Fatal(err)
	}
	if _, err := u.GetUpstreams(r); err != nil || f.callCount() != 2 {
		t.Fatalf("cache hit: calls=%d err=%v", f.callCount(), err)
	}
	if err := u.ResetCache(r); err != nil {
		t.Fatal(err)
	}
	f.set([]*api.ServiceEntry{instance("10.0.0.2", "", 80, nil)}, nil)
	got, err := u.GetUpstreams(r)
	if err != nil || len(got) != 1 || got[0].Dial != "10.0.0.2:80" || f.callCount() != 3 {
		t.Fatalf("cache reset: %v %v calls=%d", got, err, f.callCount())
	}

	u.mu.Lock()
	u.entries["api"] = cacheEntry{snapshot: snapshot{}, freshness: time.Now().Add(-time.Hour), hasResult: true}
	u.mu.Unlock()
	f.set(nil, nil)
	got, err = u.GetUpstreams(r)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty successful response: %v %v", got, err)
	}

	u.mu.Lock()
	u.entries["api"] = cacheEntry{snapshot: snapshot{all: []string{"10.0.0.1:80"}}, freshness: time.Now().Add(-time.Hour), hasResult: true}
	u.mu.Unlock()
	f.set(nil, errors.New("unavailable"))
	got, err = u.GetUpstreams(r)
	if err != nil || len(got) != 1 {
		t.Fatalf("grace: %v %v", got, err)
	}
	calls := f.callCount()
	if _, err := u.GetUpstreams(r); err != nil || f.callCount() != calls {
		t.Fatalf("grace did not suppress refresh: calls=%d err=%v", f.callCount(), err)
	}
}

func TestConcurrentFirstRefreshIsSingle(t *testing.T) {
	f := &fakeConsul{entries: []*api.ServiceEntry{instance("10.0.0.1", "", 80, nil)}}
	u := testModule(f)
	var group sync.WaitGroup
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := u.GetUpstreams(request("api")); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	if calls := f.callCount(); calls != 1 {
		t.Fatalf("got %d refreshes, want 1", calls)
	}
}

func TestLocalitySelectionRefreshesOnReset(t *testing.T) {
	f := &fakeConsul{entries: []*api.ServiceEntry{
		instance("10.0.0.1", "", 80, map[string]string{"az": "a"}),
		instance("10.0.0.2", "", 80, map[string]string{"az": "a"}),
		instance("10.0.1.1", "", 80, map[string]string{"az": "b"}),
		instance("10.0.2.1", "", 80, map[string]string{"az": "c"}),
	}}
	u := testModule(f)
	u.Locality = &Locality{NodeMetaKey: "az", localValue: "a", Minimum: 2}
	r := request("api")
	got, err := u.GetUpstreams(r)
	if err != nil || len(got) != 2 || tier(r) != "local" {
		t.Fatalf("local: %v %v %q", got, err, tier(r))
	}
	repl := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer)
	if got := repl.ReplaceAll("{http.reverse_proxy.upstreams.consul.tier}", ""); got != "local" {
		t.Fatalf("placeholder: got %q", got)
	}
	f.set([]*api.ServiceEntry{
		instance("10.0.0.1", "", 80, map[string]string{"az": "a"}),
		instance("10.0.1.1", "", 80, map[string]string{"az": "b"}),
		instance("10.0.2.1", "", 80, map[string]string{"az": "c"}),
	}, nil)
	if err := u.ResetCache(r); err != nil {
		t.Fatal(err)
	}
	got, err = u.GetUpstreams(r)
	if err != nil || len(got) != 3 || tier(r) != "all" {
		t.Fatalf("all: %v %v %q", got, err, tier(r))
	}
	if got := repl.ReplaceAll("{http.reverse_proxy.upstreams.consul.tier}", ""); got != "all" {
		t.Fatalf("placeholder after reset: got %q", got)
	}
}

func TestCacheCap(t *testing.T) {
	f := &fakeConsul{}
	u := testModule(f)
	for index := 0; index < 101; index++ {
		if _, err := u.GetUpstreams(request("service-" + strconv.Itoa(index))); err != nil {
			t.Fatal(err)
		}
	}
	if len(u.entries) != 100 {
		t.Fatalf("cache has %d entries", len(u.entries))
	}
}
