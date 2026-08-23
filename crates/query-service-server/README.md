<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Paimon Query Service

This service hosts snapshot-consistent, budgeted point-query capabilities over
a Paimon catalog. Its first capability is `BlobDescriptor` batch lookup; the
service boundary is intentionally generic so other projected fields can be
added without creating another process.

Create a JSON configuration file:

```json
{
  "listen": "127.0.0.1:8081",
  "queryTimeoutMs": 5000,
  "httpRequestTimeoutMs": 10000,
  "maxConcurrentQueries": 64,
  "maxConcurrentIndexReads": 64,
  "maxConnections": 1024,
  "httpHeaderReadTimeoutMs": 5000,
  "httpConnectionIdleTimeoutMs": 60000,
  "httpConnectionMaxAgeMs": 300000,
  "http2MaxConcurrentStreams": 64,
  "gracefulShutdownTimeoutMs": 10000,
  "queueTimeoutMs": 100,
  "maxRequestBodyBytes": 1048576,
  "maxResponseBodyBytes": 4194304,
  "readinessCacheTtlMs": 1000,
  "tableMetadataCacheTtlMs": 30000,
  "descriptorCacheTtlMs": 60000,
  "descriptorCacheMaxBytes": 67108864,
  "catalog": {
    "metastore": "filesystem",
    "warehouse": "/tmp/paimon-warehouse"
  },
  "policies": [
    {
      "table": {"database": "default", "table": "assets"},
      "keyFields": ["asset_id"],
      "blobFields": ["picture", "thumbnail"],
      "strategy": "GLOBAL_BTREE",
      "budget": {
        "maxBatchKeys": 200,
        "maxPlannedFiles": 16,
        "maxPlannedBytes": 134217728
      }
    }
  ],
  "principals": [
    {
      "name": "asset-reader",
      "bearerTokenEnv": "QUERY_SERVICE_ASSET_READER_TOKEN",
      "grants": [
        {
          "table": {"database": "default", "table": "assets"},
          "blobFields": ["picture"]
        }
      ]
    }
  ]
}
```

Start the server:

```bash
QUERY_SERVICE_ASSET_READER_TOKEN=replace-with-a-long-random-token \
QUERY_SERVICE_CONFIG=/path/to/query-service.json \
  cargo run -p paimon-query-service-server
```

`BLOB_QUERY_CONFIG` remains accepted as a compatibility fallback when
`QUERY_SERVICE_CONFIG` is unset.

Query descriptors:

```bash
curl -X POST \
  -H 'Authorization: Bearer replace-with-a-long-random-token' \
  -H 'Content-Type: application/json' \
  http://127.0.0.1:8081/api/blob/v1/databases/default/tables/assets/descriptors:batchGet \
  -d '{
    "keys": [{"asset_id": "9007199254740993"}],
    "blobFields": ["picture"],
    "descriptorFormat": "PAIMON_BASE64"
  }'
```

Lookup key values use schema-driven JSON encoding:

| Paimon key type | JSON encoding |
| --- | --- |
| `BOOLEAN` | JSON boolean |
| `TINYINT` / `SMALLINT` / `INT` | JSON integer or decimal integer string |
| `BIGINT` | Decimal integer string is recommended to avoid JSON number precision loss |
| `FLOAT` / `DOUBLE` | Finite JSON number or numeric string |
| `CHAR` / `VARCHAR` | JSON string |
| `BINARY` / `VARBINARY` | `{"base64":"..."}` |
| `DECIMAL(p,s)` | Exact decimal string without exponent notation, with at most `s` fractional digits |
| `DATE` | ISO string `YYYY-MM-DD` |
| `TIME(p)` | ISO string `HH:MM:SS[.fraction]`; lookup values currently have millisecond resolution |
| `TIMESTAMP(p)` | ISO string `YYYY-MM-DDTHH:MM:SS[.fraction]` without a time zone |
| `TIMESTAMP(p) WITH LOCAL TIME ZONE` | RFC 3339 string with `Z` or an explicit UTC offset |

Fractional seconds may not exceed the field's declared precision. The server
rejects non-finite floats and values that would require rounding or truncation.
Collection, variant, vector, and BLOB values are not supported as lookup keys.
`keys` and `blobFields` must both be non-empty.

`GLOBAL_BTREE` policies additionally require every key field to use a scalar
type supported by Paimon's sorted global indexes. In particular, binary key
types can be normalized for primary-key lookup but cannot be configured for
`GLOBAL_BTREE`. Index files are snapshot data and may have temporarily partial
coverage after writes; readiness validates table capabilities and key-type
compatibility, while per-policy planned-file and planned-byte budgets protect
the unindexed fallback path. Build or refresh the indexes with Paimon's
`create_global_index` procedure as part of the table maintenance workflow.

All HTTP errors use a JSON body:

```json
{"code":"INVALID_REQUEST","message":"keys must not be empty"}
```

Stable transport-level codes include `INVALID_JSON`, `INVALID_REQUEST`,
`UNSUPPORTED_MEDIA_TYPE`, `REQUEST_TOO_LARGE`, `ROUTE_NOT_FOUND`, and
`METHOD_NOT_ALLOWED`. `REQUEST_TIMEOUT` covers the complete HTTP lifecycle,
including slow request uploads; `RESPONSE_TOO_LARGE` rejects a successful
lookup whose serialized JSON exceeds the configured response envelope. The
`x-request-id` response header is present on these errors as well as successful
responses.

Each principal receives explicit table and optional BLOB-field grants. Tokens
must contain at least 16 bytes and are retained by the running service only as
SHA-256 hashes; `bearerTokenEnv` avoids storing the secret in the JSON file.
Using `bearerToken` inside a principal is also supported. The legacy top-level
`bearerToken`/`bearerTokenEnv` grants access to all configured policies, while
anonymous access is disabled by default. To run without authentication, set
`"allowAnonymous": true` explicitly; it cannot be combined with bearer-token
authentication. Unknown configuration fields, including nested policy and grant
fields, fail startup so security-sensitive spelling mistakes cannot broaden
access.

Missing or invalid credentials receive `401` with `WWW-Authenticate: Bearer`;
authenticated requests outside their grants receive `403`. Continue to place
the service behind TLS and the platform's normal identity boundary.

Every response includes an `x-request-id` header. A valid caller-provided ID is
preserved; otherwise the server generates one. Access logs are emitted as JSON
through a bounded non-blocking queue to stderr and contain the request ID,
authenticated principal, method, path, status, and elapsed time. When stderr
cannot keep up, log events are dropped rather than blocking Tokio workers; the
dropped count is exported in Prometheus metrics.

`maxConcurrentQueries` limits admitted descriptor requests per process and is
acquired before the JSON body is read. A request that cannot enter within
`queueTimeoutMs` receives `429 SERVER_BUSY`; a lookup that
exceeds `queryTimeoutMs` receives `504 QUERY_TIMEOUT`. The JSON body limit is
controlled by `maxRequestBodyBytes`, and successful JSON responses are bounded
by `maxResponseBodyBytes`. `httpRequestTimeoutMs` is an outer deadline covering
request upload, admission, lookup, and response construction. Keep it larger
than `queueTimeoutMs + queryTimeoutMs`. The values above are also the defaults.
`maxConcurrentIndexReads` bounds the product of admitted queries and each
query's global-index scanner width; it must be at least `maxConcurrentQueries`.

At the transport layer, `maxConnections` bounds parsed and partially parsed TCP
connections, `httpHeaderReadTimeoutMs` closes slow HTTP/1 header uploads, and
also bounds the initial HTTP/1 versus HTTP/2 protocol detection.
`httpConnectionIdleTimeoutMs` closes read-idle HTTP/1 and HTTP/2 connections and
must exceed `httpRequestTimeoutMs`. At `httpConnectionMaxAgeMs`, the server
starts a graceful connection shutdown (HTTP/2 GOAWAY) and gives active work up
to `httpRequestTimeoutMs` to drain. This bounds HTTP/2 clients which continuously
drip incomplete frame or header bytes without resetting valid in-flight work;
the max age must exceed `httpConnectionIdleTimeoutMs`.
`http2MaxConcurrentStreams` bounds multiplexing per HTTP/2 connection.
`gracefulShutdownTimeoutMs` bounds connection drain after SIGTERM or Ctrl-C.

Operational endpoints do not require the bearer token:

- `GET /healthz` is a process liveness probe.
- `GET /readyz` reloads and validates every configured table, including catalog
  connectivity and lookup-policy compatibility. Results are reused for
  `readinessCacheTtlMs`, and concurrent refreshes collapse into one catalog
  traversal; set the TTL to `0` to force every probe to refresh.
- `GET /metrics` exposes Prometheus text metrics for HTTP status classes,
  latency histograms, authentication failures, admission, timeouts, in-flight work,
  lookup outcomes, planned files/bytes, readiness refresh/cache activity,
  total HTTP timeouts, oversized responses, and table/descriptor-cache activity.

Keep these operational endpoints on an internal network or restrict them at the
ingress layer; `/readyz` deliberately performs catalog metadata I/O.

Table metadata is cached per process for `tableMetadataCacheTtlMs`; set it to
`0` to disable caching. Snapshot IDs are still resolved and pinned per request,
and a readiness probe refreshes all cached table entries.

Successful descriptor results are cached per process for
`descriptorCacheTtlMs`, bounded by `descriptorCacheMaxBytes`. Set either value
to `0` to disable this cache. The cache key contains the resolved snapshot ID,
schema ID, table location, immutable snapshot metadata fingerprint, requested
keys, BLOB fields, and descriptor format,
so a request for the latest data resolves and pins its snapshot before cache
lookup. Concurrent identical misses are collapsed into one scan. Responses
include `cacheHit`; `scan` describes the original plan, while Prometheus
planned-file/byte counters count only scans actually executed by this process.
A single cached entry is also bounded by `maxResponseBodyBytes`, so an oversized
result that cannot be returned is never retained in the descriptor cache.
Miss coalescing is independent of long-term cache admission, so concurrent
identical oversized results and failures also execute only one scan.
