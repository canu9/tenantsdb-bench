# TenantsDB Benchmark

Measures proxy overhead, throughput, and latency across TenantsDB's database proxy layer.

## What It Tests

- **Proxy Overhead** — How much latency does the TenantsDB proxy add vs a direct database connection?
- **Throughput** — How many queries per second can a tenant sustain through the proxy?
- **Latency Distribution** — p50, p95, p99 latency under concurrent load

## Workload

80% reads (SELECT by primary key) / 20% writes (UPDATE balance). Configurable concurrency and query count.

## Setup

```bash
git clone <repo-url>
cd tenantsdb-bench
go mod tidy
go build -o bench .
```

## Usage

```bash
# Proxy overhead: compares direct DB vs through proxy
./bench -test overhead \
  -proxy-host <proxy-ip> -proxy-port <port> \
  -proxy-user <project-id> -proxy-pass <proxy-password> \
  -proxy-db <tenant-database> \
  -direct-host <db-ip> -direct-port <port> \
  -direct-user <db-user> -direct-pass <db-password> \
  -direct-db <tenant-database>

# Throughput only: measures QPS through proxy
./bench -test throughput \
  -proxy-host <proxy-ip> -proxy-port <port> \
  -proxy-user <project-id> -proxy-pass <proxy-password> \
  -proxy-db <tenant-database>
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `-db` | postgres | Database type: postgres, mysql, mongodb, redis |
| `-test` | overhead | Test type: overhead, throughput |
| `-queries` | 10000 | Total queries to run |
| `-concurrency` | 10 | Parallel connections |
| `-warmup` | 100 | Warmup queries before measuring |
| `-seed-rows` | 10000 | Rows to insert for test data |# TenantsDB Benchmark

Measures proxy overhead, throughput, and latency across TenantsDB's database proxy layer.

## What It Tests

- **Proxy Overhead** — How much latency does the TenantsDB proxy add vs a direct database connection?
- **Throughput** — How many queries per second can a tenant sustain through the proxy?
- **Latency Distribution** — p50, p95, p99 latency under concurrent load

## Workload

80% reads (SELECT by primary key) / 20% writes (UPDATE balance). Configurable concurrency and query count.

## Setup

```bash
git clone <repo-url>
cd tenantsdb-bench
go mod tidy
go build -o bench .
```

## Usage

```bash
# Proxy overhead: compares direct DB vs through proxy
./bench -test overhead \
  -proxy-host <proxy-ip> -proxy-port <port> \
  -proxy-user <project-id> -proxy-pass <proxy-password> \
  -proxy-db <tenant-database> \
  -direct-host <db-ip> -direct-port <port> \
  -direct-user <db-user> -direct-pass <db-password> \
  -direct-db <tenant-database>

# Throughput only: measures QPS through proxy
./bench -test throughput \
  -proxy-host <proxy-ip> -proxy-port <port> \
  -proxy-user <project-id> -proxy-pass <proxy-password> \
  -proxy-db <tenant-database>
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `-db` | postgres | Database type: postgres, mysql, mongodb, redis |
| `-test` | overhead | Test type: overhead, throughput |
| `-queries` | 10000 | Total queries to run |
| `-concurrency` | 10 | Parallel connections |
| `-warmup` | 100 | Warmup queries before measuring |
| `-seed-rows` | 10000 | Rows to insert for test data |