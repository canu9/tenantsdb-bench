# TenantsDB Benchmark

Measures proxy overhead, throughput, and latency for TenantsDB.

## Setup

```bash
# On node02
git clone <repo-url> ~/tenantsdb-bench
cd ~/tenantsdb-bench
go mod tidy
go build -o bench .
```

## Test 1: Proxy Overhead (Direct vs Proxied)

```bash
./bench \
  -test overhead \
  -proxy-host 192.168.60.13 \
  -proxy-port 30432 \
  -proxy-user tdb_24cbcee9 \
  -proxy-pass tdb_7288175b98bafbae \
  -proxy-db bench_pg__bench01 \
  -direct-host 192.168.60.12 \
  -direct-port 5432 \
  -direct-user <pg_user> \
  -direct-pass <pg_pass> \
  -direct-db bench_pg__bench01 \
  -queries 10000 \
  -concurrency 10
```

## Test 2: Throughput Only

```bash
./bench \
  -test throughput \
  -proxy-host 192.168.60.13 \
  -proxy-port 30432 \
  -proxy-user tdb_24cbcee9 \
  -proxy-pass tdb_7288175b98bafbae \
  -proxy-db bench_pg__bench01 \
  -queries 50000 \
  -concurrency 50
```

## Output

Produces latency distribution (p50/p95/p99), QPS, and proxy overhead comparison.