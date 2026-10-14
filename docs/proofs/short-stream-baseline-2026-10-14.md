# Loopback short-stream baseline — 2026-10-14

The released **0.2.41+84c3208** runtime already meets the cold browser bound in
this local replay, but misses the 1.5× bound for short connections and the 2×
bound for four concurrent 1 MiB streams. This does not reproduce or resolve
the reported two-machine cold-load gap. No throughput optimization is claimed.

| Workload | Plain TCP p50 ms | Fabric p50 ms | Fabric / TCP | Result |
| --- | ---: | ---: | ---: | --- |
| 100 × 1 KiB | 0.333 | 0.796 | 2.39× | miss |
| 100 × 16 KiB | 0.388 | 0.947 | 2.44× | miss |
| 100 × 256 KiB | 0.648 | 1.968 | 3.04× | miss |
| 4 × 256 KiB (1 MiB total), 20 trials | 3.051 | 5.994 | 1.96× | record only |
| 4 × 1 MiB, 20 trials | 4.860 | 14.291 | 2.94× | miss |
| Browser cold, 10 paired contexts | 2477.490 | 2579.820 | 1.04× | within 1.5× |
| Browser warm, 10 paired contexts | 33.119 | 32.376 | 0.98× | record only |

Both carriers use the same Python HTTP/1.1 backend on loopback. Short trials
alternate carrier order and create a fresh local socket per request, including
connect, request, response and EOF in total time. The Fabric peer connection
is warmed before measurement. Four-stream trials start four fresh sockets and
time all four completions. Raw samples and first response-byte measurements are
in [the JSON receipt](short-stream-baseline-2026-10-14.json).

The browser graph has one document and 360 deferred 47,500-byte scripts
(17.1 MB of script payload). Every cold trial creates a fresh Chromium context;
its warm navigation uses that same context. Carriers alternate order each
trial. All 360 script markers must execute, so failed assets cannot produce a
fast accepted load. An inline icon avoids an unrelated favicon request.
Chromium version: **149.0.7827.55**. Cold refers to the browser
HTTP cache; the underlying Fabric connection is warm.

The probe recorded `supported` over a `direct` path. This is a same-machine
measurement, with no forced relay or remote-machine claim. The backend uses its
stock listen backlog and leaves TCP_NODELAY off on accepted service sockets;
the JSON records those settings, platform, binary hash and host load. The
machine was shared and had background activity, including build work. Pairing
limits carrier-order bias but cannot remove scheduler noise. These are baseline
measurements, not a release acceptance claim.

The browser comparison must not be used to infer a Hello, target-open or Nagle
cause. The next profile must split tunnel Hello admission, local target open,
first response byte and flow-control stalls, and compare those intervals to
the short-stream misses. The earlier scratch TCP_NODELAY trial did not show a
consistent benefit and has no retained source change.

## Repeat

Use a released binary whose archive checksum has been verified. The benchmark
creates fresh homes and cleans up only its own two foreground daemon processes.
Python 3 is sufficient for short and concurrent streams:

```sh
bin/fabric-short-stream-baseline --binary ./fabric --output baseline.json
```

For the browser graph, install Playwright in a disposable directory and make it
resolvable to Node; select a compatible Chromium executable if required:

```sh
npm install --prefix /tmp/fabric-bench-tools playwright
NODE_PATH=/tmp/fabric-bench-tools/node_modules CHROMIUM=/path/to/chromium \
  bin/fabric-short-stream-baseline --binary ./fabric --browser --output baseline.json
```

The command records performance misses rather than turning machine-dependent
timings into a CI test. It does fail for a bad response, incomplete asset graph,
a non-direct path, or a daemon that cannot start. Later release proofs should
retain their exact runtime version and repeat the same paired workloads.
