package pg

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"tenantsdb-bench/bench"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RunMultiTenant(proxyCfg bench.ConnConfig, params bench.BenchParams) {
	tenants := []string{
		"bench_pg__bench01", "bench_pg__bench02", "bench_pg__bench03",
		"bench_pg__bench04", "bench_pg__bench05", "bench_pg__bench06",
		"bench_pg__bench07", "bench_pg__bench08", "bench_pg__bench09",
		"bench_pg__bench10",
	}

	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  PostgreSQL Multi-Tenant Benchmark")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Tenants: %d | Total queries: %d | Total concurrency: %d\n",
		len(tenants), params.Queries, params.Concurrency)
	fmt.Printf("  Per tenant: %d queries, %d concurrent\n\n",
		params.Queries/len(tenants), params.Concurrency/len(tenants))

	pools := make([]*pgxpool.Pool, len(tenants))
	for i, t := range tenants {
		cfg := proxyCfg
		cfg.Database = t
		fmt.Printf("  [%d/%d] Connecting to %s...\n", i+1, len(tenants), t)
		pool, err := Connect(cfg, "disable")
		if err != nil {
			fmt.Printf("  ✗ Failed: %v\n", err)
			return
		}
		defer pool.Close()
		pools[i] = pool

		if err := SeedData(pool, params.SeedRows); err != nil {
			fmt.Printf("  ✗ Seed failed: %v\n", err)
			return
		}
	}
	fmt.Println("  ✓ All tenants connected and seeded\n")

	fmt.Println("── Running multi-tenant benchmark ──")
	queriesPerTenant := params.Queries / len(tenants)
	concPerTenant := params.Concurrency / len(tenants)
	if concPerTenant < 1 {
		concPerTenant = 1
	}

	results := make([]bench.QueryResult, params.Queries)
	maxID := params.SeedRows

	start := time.Now()
	var wg sync.WaitGroup

	for t := 0; t < len(tenants); t++ {
		pool := pools[t]
		tenantOffset := t * queriesPerTenant

		for w := 0; w < concPerTenant; w++ {
			wg.Add(1)
			workerQueries := queriesPerTenant / concPerTenant
			workerOffset := tenantOffset + (w * workerQueries)

			go func(p *pgxpool.Pool, offset, count int) {
				defer wg.Done()
				ctx := context.Background()

				for i := 0; i < count; i++ {
					idx := offset + i
					qStart := time.Now()

					if rand.Intn(100) < 80 {
						id := rand.Intn(maxID) + 1
						var rID int
						var rName string
						var rBalance float64
						err := p.QueryRow(ctx, "SELECT id, name, balance FROM accounts WHERE id = $1", id).Scan(&rID, &rName, &rBalance)
						results[idx] = bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err}
					} else {
						id := rand.Intn(maxID) + 1
						delta := rand.Float64()*200 - 100
						_, err := p.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", delta, id)
						results[idx] = bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err}
					}
				}
			}(pool, workerOffset, workerQueries)
		}
	}
	wg.Wait()

	totalDuration := time.Since(start)

	errCount := 0
	for _, r := range results {
		if r.Err != nil && errCount < 5 {
			fmt.Printf("  ⚠ Error: %v\n", r.Err)
			errCount++
		}
	}

	stats := bench.ComputeStats(
		fmt.Sprintf("Multi-Tenant (%d tenants, %d total concurrent)", len(tenants), params.Concurrency),
		results, totalDuration)
	bench.PrintStats(stats)
}