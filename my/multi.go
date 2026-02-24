package my

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"tenantsdb-bench/bench"
)

func RunMultiTenant(proxyCfg bench.ConnConfig, params bench.BenchParams) {
	tenants := []string{
		"bench_mysql__bench01", "bench_mysql__bench02", "bench_mysql__bench03",
		"bench_mysql__bench04", "bench_mysql__bench05", "bench_mysql__bench06",
		"bench_mysql__bench07", "bench_mysql__bench08", "bench_mysql__bench09",
		"bench_mysql__bench10",
	}

	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  MySQL Multi-Tenant Benchmark")
	fmt.Println("═══════════════════════════════════════════")
	if params.Duration > 0 {
		fmt.Printf("  Tenants: %d | Duration: %s | Concurrency: %d\n\n",
			len(tenants), params.Duration, params.Concurrency)
	} else {
		fmt.Printf("  Tenants: %d | Total queries: %d | Total concurrency: %d\n",
			len(tenants), params.Queries, params.Concurrency)
		fmt.Printf("  Per tenant: %d queries, %d concurrent\n\n",
			params.Queries/len(tenants), params.Concurrency/len(tenants))
	}

	pools := make([]*sql.DB, len(tenants))
	for i, t := range tenants {
		cfg := proxyCfg
		cfg.Database = t
		fmt.Printf("  [%d/%d] Connecting to %s...\n", i+1, len(tenants), t)
		db, err := Connect(cfg)
		if err != nil {
			fmt.Printf("  ✗ Failed: %v\n", err)
			return
		}
		defer db.Close()
		pools[i] = db

		if err := SeedData(db, params.SeedRows); err != nil {
			fmt.Printf("  ✗ Seed failed: %v\n", err)
			return
		}
	}
	fmt.Println("  ✓ All tenants connected and seeded\n")

	fmt.Println("── Running multi-tenant benchmark ──")

	runOnce := func(run int) bench.BenchStats {
		if params.Duration > 0 {
			return runMultiTimed(pools, tenants, params)
		}
		return runMultiCount(pools, tenants, params)
	}

	var stats bench.BenchStats
	if params.Runs > 1 {
		stats = bench.RunMultiple(params.Runs,
			fmt.Sprintf("Multi-Tenant (%d tenants)", len(tenants)), runOnce)
	} else {
		stats = runOnce(0)
	}
	bench.PrintStats(stats)
}

func runMultiCount(pools []*sql.DB, tenants []string, params bench.BenchParams) bench.BenchStats {
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
		db := pools[t]
		tenantOffset := t * queriesPerTenant

		for w := 0; w < concPerTenant; w++ {
			wg.Add(1)
			workerQueries := queriesPerTenant / concPerTenant
			workerOffset := tenantOffset + (w * workerQueries)

			go func(d *sql.DB, offset, count int) {
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
						err := d.QueryRowContext(ctx, "SELECT id, name, balance FROM accounts WHERE id = ?", id).Scan(&rID, &rName, &rBalance)
						results[idx] = bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err}
					} else {
						id := rand.Intn(maxID) + 1
						delta := rand.Float64()*200 - 100
						_, err := d.ExecContext(ctx, "UPDATE accounts SET balance = balance + ? WHERE id = ?", delta, id)
						results[idx] = bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err}
					}
				}
			}(db, workerOffset, workerQueries)
		}
	}
	wg.Wait()

	totalDuration := time.Since(start)
	return bench.ComputeStats(
		fmt.Sprintf("Multi-Tenant (%d tenants, %d concurrent)", len(tenants), params.Concurrency),
		results, totalDuration)
}

func runMultiTimed(pools []*sql.DB, tenants []string, params bench.BenchParams) bench.BenchStats {
	concPerTenant := params.Concurrency / len(tenants)
	if concPerTenant < 1 {
		concPerTenant = 1
	}
	maxID := params.SeedRows

	var mu sync.Mutex
	var results []bench.QueryResult
	var stopped atomic.Bool

	start := time.Now()
	time.AfterFunc(params.Duration, func() { stopped.Store(true) })

	var wg sync.WaitGroup
	for t := 0; t < len(tenants); t++ {
		db := pools[t]
		for w := 0; w < concPerTenant; w++ {
			wg.Add(1)
			go func(d *sql.DB) {
				defer wg.Done()
				ctx := context.Background()
				var local []bench.QueryResult

				for !stopped.Load() {
					qStart := time.Now()
					if rand.Intn(100) < 80 {
						id := rand.Intn(maxID) + 1
						var rID int
						var rName string
						var rBalance float64
						err := d.QueryRowContext(ctx, "SELECT id, name, balance FROM accounts WHERE id = ?", id).Scan(&rID, &rName, &rBalance)
						local = append(local, bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err})
					} else {
						id := rand.Intn(maxID) + 1
						delta := rand.Float64()*200 - 100
						_, err := d.ExecContext(ctx, "UPDATE accounts SET balance = balance + ? WHERE id = ?", delta, id)
						local = append(local, bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err})
					}
				}

				mu.Lock()
				results = append(results, local...)
				mu.Unlock()
			}(db)
		}
	}
	wg.Wait()

	totalDuration := time.Since(start)
	return bench.ComputeStats(
		fmt.Sprintf("Multi-Tenant (%d tenants, %d concurrent)", len(tenants), params.Concurrency),
		results, totalDuration)
}