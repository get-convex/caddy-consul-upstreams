# Caddy Consul upstreams

`caddy-consul-upstreams` is a Caddy v2 dynamic-upstream module that discovers
passing Consul services. It is deliberately limited to service discovery; it
does not generate routes or configure Consul.

## Install

Build Caddy with [xcaddy](https://github.com/caddyserver/xcaddy):

```sh
xcaddy build v2.11.4 --with github.com/get-convex/caddy-consul-upstreams@v0.1.0
```

For local development, replace `@v0.1.0` with `=.`.

v0.1 is compatible with Caddy 2.11.4.

## Caddyfile

```caddyfile
example.com {
	reverse_proxy {
		dynamic consul {
			address http://127.0.0.1:8500
			service {usher_service}
			refresh 10s
			grace_period 30s
			locality {
				node_meta availability-zone-id
				minimum 2
				fallback_on_retry
			}
		}
	}
}
```

`service` accepts request placeholders. The defaults are
`address http://127.0.0.1:8500`, `refresh 1m`, `grace_period 0s`, and locality
`minimum 2`.

## JSON

```json
{
  "source": "consul",
  "address": "http://127.0.0.1:8500",
  "service": "usher",
  "refresh": 10000000000,
  "grace_period": 30000000000,
  "locality": {
    "node_meta_key": "availability-zone-id",
    "minimum_preferred": 2,
    "fallback_on_retry": true
  }
}
```

The module queries Consul's passing-only health endpoint synchronously on the
first request for a service and once per `refresh` interval thereafter. Results
are cached by expanded service name, with at most 100 service entries. A
successful empty result is authoritative. If a refresh fails and a prior result
exists, `grace_period` continues serving that result before attempting another
refresh.

With locality, the module reads the configured local metadata from the Consul
agent's `Config.NodeMeta` during provisioning. If at least `minimum` passing
instances have the same metadata, normal attempts use only those instances.
Otherwise all passing instances are returned. Missing target metadata is
nonlocal; missing local metadata fails provisioning.

`fallback_on_retry` makes a retry after Caddy calls `ResetCache` select only
nonlocal passing instances when the previous selection was `preferred`. If
there are no nonlocal instances, it remains on the preferred pool. A retry
following `all`, `fallback`, or `retry_fallback` preserves that tier.

The request placeholder `{http.reverse_proxy.upstreams.consul.tier}` is one of
`all`, `preferred`, `fallback`, or `retry_fallback`.

## Scope

v0.1 intentionally has no tag locality, latency ordering, weights, WAN
federation, arbitrary preference expressions, or custom metrics.
