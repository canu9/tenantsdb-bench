# TenantsDB Benchmark

Measures proxy overhead, throughput, and latency across TenantsDB's database proxy layer.

## What It Tests

- **Proxy Overhead**: How much latency does the TenantsDB proxy add vs a direct database connection?
- **Throughput**: How many queries per second can a tenant sustain through the proxy?
- **Latency Distribution**: p50, p75, p90, p95, p99 latency under concurrent load

## Methodology

- **Workload**: 80% reads (SELECT by primary key) / 20% writes (UPDATE balance)
- **Standards**: TPC-B baseline + custom multi-tenant write workload
- **Reproducibility**: Every test executed 5 times, median reported (SPEC-compliant methodology)
- **Configuration**: Stock database settings, no tuning, no query optimization
- **Warm-up**: Configurable warm-up queries excluded from measurements

## Setup

```bash
git clone <repo-url>
cd tenantsdb-bench
go mod tidy
go build -o bench .
```

## Usage

### Proxy Overhead Test

Compares direct database connection vs through the TenantsDB proxy.

```bash
./bench -test overhead \
  -proxy-host <proxy-ip> -proxy-port <proxy-port> \
  -proxy-user <project-id> -proxy-pass <proxy-password> \
  -proxy-db <tenant-database> \
  -direct-host <db-ip> -direct-port <db-port> \
  -direct-user <db-user> -direct-pass <db-password> \
  -direct-db <tenant-database>
```

### Throughput Test

Measures sustained QPS through the proxy for a single tenant.

```bash
./bench -test throughput \
  -proxy-host <proxy-ip> -proxy-port <proxy-port> \
  -proxy-user <project-id> -proxy-pass <proxy-password> \
  -proxy-db <tenant-database>
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `-db` | `postgres` | Database type: `postgres`, `mysql`, `mongodb`, `redis` |
| `-test` | `overhead` | Test type: `overhead`, `throughput` |
| `-queries` | `10000` | Total queries to run |
| `-concurrency` | `10` | Parallel connections |
| `-warmup` | `100` | Warm-up queries before measuring |
| `-seed-rows` | `10000` | Rows to insert for test data |

## Output

Results include per-query latency percentiles (p50/p75/p90/p95/p99), total QPS, error rate, and proxy overhead delta when running the overhead test.

## License

Proprietary. Copyright Binary Leap OÜ.