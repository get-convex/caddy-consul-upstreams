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
    "node_meta": "availability-zone-id",
    "minimum": 2
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
agent's `Config.NodeMeta` during provisioning. For example, a Caddy instance
in AZ `a` with passing Ushers in `a`, `a`, `b`, and `c` uses only the two `a`
instances when `minimum` is 2. If fewer than two `a` instances are passing, it
uses all passing instances. Missing target metadata is nonlocal; missing local
metadata fails provisioning. Retries re-query Consul and apply the same rule.

The request placeholder `{http.reverse_proxy.upstreams.consul.tier}` is either
`local` or `all`.

## Scope

v0.1 intentionally has no tag locality, latency ordering, weights, WAN
federation, arbitrary preference expressions, or custom metrics.
