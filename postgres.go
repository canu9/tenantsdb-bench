package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func pgConnect(c ConnConfig, sslmode string) (*pgxpool.Pool, error) {
	if sslmode == "" {
		sslmode = "disable"
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, sslmode)

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 10
	config.MinConns = 2

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func pgSeedData(pool *pgxpool.Pool, rows int) error {
	ctx := context.Background()
	var count int
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM accounts").Scan(&count)
	if err != nil {
		return fmt.Errorf("seed check: %w", err)
	}
	if count >= rows {
		fmt.Printf("  Data already seeded (%d rows)\n", count)
		return nil
	}

	fmt.Printf("  Seeding %d rows...\n", rows)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO accounts (name, balance)
		SELECT 'user_' || i, (random() * 10000)::decimal(15,2)
		FROM generate_series(1, %d) i
		ON CONFLICT DO NOTHING
	`, rows))
	return err
}

func pgRunQueries(pool *pgxpool.Pool, params BenchParams, label string) BenchStats {
	ctx := context.Background()
	maxID := params.SeedRows

	// Warmup
	fmt.Printf("  Warming up (%d queries)...\n", params.Warmup)
	for i := 0; i < params.Warmup; i++ {
		id := rand.Intn(maxID) + 1
		pool.QueryRow(ctx, "SELECT id, name, balance FROM accounts WHERE id = $1", id).Scan(new(int), new(string), new(float64))
	}

	// Benchmark
	fmt.Printf("  Running %d queries (%d concurrent)...\n", params.Queries, params.Concurrency)

	results := make([]QueryResult, params.Queries)
	queriesPerWorker := params.Queries / params.Concurrency

	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < params.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			offset := workerID * queriesPerWorker

			for i := 0; i < queriesPerWorker; i++ {
				idx := offset + i
				qStart := time.Now()

				// 80% reads, 20% writes
				if rand.Intn(100) < 80 {
					id := rand.Intn(maxID) + 1
					var rID int
					var rName string
					var rBalance float64
					err := pool.QueryRow(ctx, "SELECT id, name, balance FROM accounts WHERE id = $1", id).Scan(&rID, &rName, &rBalance)
					results[idx] = QueryResult{Duration: time.Since(qStart), Err: err}
				} else {
					id := rand.Intn(maxID) + 1
					delta := rand.Float64()*200 - 100
					_, err := pool.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", delta, id)
					results[idx] = QueryResult{Duration: time.Since(qStart), Err: err}
				}
			}
		}(w)
	}
	wg.Wait()

	totalDuration := time.Since(start)

	// Log first few errors
	errCount := 0
	for _, r := range results {
		if r.Err != nil && errCount < 5 {
			fmt.Printf("  ⚠ Error: %v\n", r.Err)
			errCount++
		}
	}

	return ComputeStats(label, results, totalDuration)
}

func RunPostgresOverhead(proxyCfg, directCfg ConnConfig, params BenchParams) {
	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  PostgreSQL Proxy Overhead Benchmark")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Queries: %d | Concurrency: %d | Workload: 80%% read / 20%% write\n\n", params.Queries, params.Concurrency)

	// Connect direct
	fmt.Println("[1/4] Connecting directly to PostgreSQL...")
	directPool, err := pgConnect(directCfg, "disable")
	if err != nil {
		fmt.Printf("  ✗ Direct connection failed: %v\n", err)
		return
	}
	defer directPool.Close()
	fmt.Println("  ✓ Connected")

	// Seed data direct
	fmt.Println("\n[2/4] Seeding test data (direct)...")
	if err := pgSeedData(directPool, params.SeedRows); err != nil {
		fmt.Printf("  ✗ Seed failed: %v\n", err)
		return
	}
	fmt.Println("  ✓ Data ready")

	// Connect proxy
	fmt.Println("\n[3/4] Connecting through TenantsDB proxy...")
	proxyPool, err := pgConnect(proxyCfg, "disable")
	if err != nil {
		fmt.Printf("  ✗ Proxy connection failed: %v\n", err)
		return
	}
	defer proxyPool.Close()
	fmt.Println("  ✓ Connected")

	// Run benchmarks
	fmt.Println("\n[4/4] Running benchmarks...")
	fmt.Println("\n── Direct PostgreSQL ──")
	directStats := pgRunQueries(directPool, params, "Direct PostgreSQL")
	PrintStats(directStats)

	fmt.Println("\n── Through TenantsDB Proxy ──")
	proxyStats := pgRunQueries(proxyPool, params, "Through TenantsDB Proxy")
	PrintStats(proxyStats)

	// Comparison
	PrintComparison(proxyStats, directStats)
}

func RunPostgresThroughput(proxyCfg ConnConfig, params BenchParams) {
	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  PostgreSQL Throughput Benchmark")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Queries: %d | Concurrency: %d\n\n", params.Queries, params.Concurrency)

	fmt.Println("[1/3] Connecting through TenantsDB proxy...")
	pool, err := pgConnect(proxyCfg, "disable")
	if err != nil {
		fmt.Printf("  ✗ Connection failed: %v\n", err)
		return
	}
	defer pool.Close()
	fmt.Println("  ✓ Connected")

	fmt.Println("\n[2/3] Seeding test data...")
	if err := pgSeedData(pool, params.SeedRows); err != nil {
		fmt.Printf("  ✗ Seed failed: %v\n", err)
		return
	}
	fmt.Println("  ✓ Data ready")

	fmt.Println("\n[3/3] Running benchmark...")
	stats := pgRunQueries(pool, params, "PostgreSQL Throughput (via Proxy)")
	PrintStats(stats)
}

func RunPostgresMultiTenant(proxyCfg ConnConfig, params BenchParams) {
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
		pool, err := pgConnect(cfg, "disable")
		if err != nil {
			fmt.Printf("  ✗ Failed: %v\n", err)
			return
		}
		defer pool.Close()
		pools[i] = pool

		if err := pgSeedData(pool, params.SeedRows); err != nil {
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

	results := make([]QueryResult, params.Queries)
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
						results[idx] = QueryResult{Duration: time.Since(qStart), Err: err}
					} else {
						id := rand.Intn(maxID) + 1
						delta := rand.Float64()*200 - 100
						_, err := p.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", delta, id)
						results[idx] = QueryResult{Duration: time.Since(qStart), Err: err}
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

	stats := ComputeStats(
		fmt.Sprintf("Multi-Tenant (%d tenants, %d total concurrent)", len(tenants), params.Concurrency),
		results, totalDuration)
	PrintStats(stats)
}

func RunPostgresIsolation(proxyCfg ConnConfig, params BenchParams) {
	victim := "bench_pg__bench01"
	noisy := []string{
		"bench_pg__bench02", "bench_pg__bench03", "bench_pg__bench04",
		"bench_pg__bench05", "bench_pg__bench06", "bench_pg__bench07",
		"bench_pg__bench08", "bench_pg__bench09", "bench_pg__bench10",
	}

	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  PostgreSQL Noisy Neighbor Isolation Test")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Victim tenant: %s\n", victim)
	fmt.Printf("  Noisy tenants: %d (each hammering with writes)\n\n", len(noisy))

	// Connect victim
	fmt.Println("[1/3] Connecting victim tenant...")
	victimCfg := proxyCfg
	victimCfg.Database = victim
	victimPool, err := pgConnect(victimCfg, "disable")
	if err != nil {
		fmt.Printf("  ✗ Failed: %v\n", err)
		return
	}
	defer victimPool.Close()
	if err := pgSeedData(victimPool, params.SeedRows); err != nil {
		fmt.Printf("  ✗ Seed failed: %v\n", err)
		return
	}
	fmt.Println("  ✓ Victim ready")

	// Connect noisy tenants
	fmt.Println("\n[2/3] Connecting noisy tenants...")
	noisyPools := make([]*pgxpool.Pool, len(noisy))
	for i, t := range noisy {
		cfg := proxyCfg
		cfg.Database = t
		pool, err := pgConnect(cfg, "disable")
		if err != nil {
			fmt.Printf("  ✗ %s failed: %v\n", t, err)
			return
		}
		defer pool.Close()
		noisyPools[i] = pool

		if err := pgSeedData(pool, params.SeedRows); err != nil {
			fmt.Printf("  ✗ Seed %s failed: %v\n", t, err)
			return
		}
	}
	fmt.Println("  ✓ All noisy tenants ready")

	fmt.Println("\n[3/3] Running isolation test...")
	maxID := params.SeedRows
	victimQueries := params.Queries
	victimConc := 5

	// ── Phase 1: Victim alone ──
	fmt.Println("\n── Phase 1: Victim alone (no noise) ──")
	baselineStats := pgRunQueries(victimPool, BenchParams{
		Queries:     victimQueries,
		Concurrency: victimConc,
		Warmup:      params.Warmup,
		SeedRows:    params.SeedRows,
	}, "Victim ALONE")
	PrintStats(baselineStats)

	// ── Phase 2: Victim under noise ──
	fmt.Println("\n── Phase 2: Starting noisy neighbors ──")
	fmt.Printf("  Launching %d noisy tenants (heavy writes)...\n", len(noisy))

	stopNoise := make(chan struct{})
	var noiseWg sync.WaitGroup

	// Start noisy workers — 5 concurrent per tenant, 100% writes
	for _, pool := range noisyPools {
		for w := 0; w < 5; w++ {
			noiseWg.Add(1)
			go func(p *pgxpool.Pool) {
				defer noiseWg.Done()
				ctx := context.Background()
				for {
					select {
					case <-stopNoise:
						return
					default:
						id := rand.Intn(maxID) + 1
						delta := rand.Float64()*200 - 100
						p.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", delta, id)
					}
				}
			}(pool)
		}
	}

	// Let noise ramp up
	time.Sleep(2 * time.Second)
	fmt.Println("  ✓ Noise running (9 tenants × 5 concurrent = 45 writers)")

	fmt.Println("\n── Measuring victim under noise ──")
	noiseStats := pgRunQueries(victimPool, BenchParams{
		Queries:     victimQueries,
		Concurrency: victimConc,
		Warmup:      params.Warmup,
		SeedRows:    params.SeedRows,
	}, "Victim UNDER NOISE")
	PrintStats(noiseStats)

	// Stop noise
	close(stopNoise)
	noiseWg.Wait()

	// ── Comparison ──
	fmt.Println()
	fmt.Println("╔═════════════════════════════════════════════════════════════╗")
	fmt.Println("║  NOISY NEIGHBOR ISOLATION RESULTS                          ║")
	fmt.Println("╠═══════════════════╦════════════════╦════════════════════════╣")
	fmt.Println("║  Metric           ║  Alone         ║  Under Noise           ║")
	fmt.Println("╠═══════════════════╬════════════════╬════════════════════════╣")
	fmt.Printf("║  QPS              ║  %-13.1f ║  %-23.1f║\n", baselineStats.QPS, noiseStats.QPS)
	fmt.Printf("║  Latency avg      ║  %-13s ║  %-23s║\n", fmtDuration(baselineStats.Avg), fmtDuration(noiseStats.Avg))
	fmt.Printf("║  Latency p50      ║  %-13s ║  %-23s║\n", fmtDuration(baselineStats.P50), fmtDuration(noiseStats.P50))
	fmt.Printf("║  Latency p95      ║  %-13s ║  %-23s║\n", fmtDuration(baselineStats.P95), fmtDuration(noiseStats.P95))
	fmt.Printf("║  Latency p99      ║  %-13s ║  %-23s║\n", fmtDuration(baselineStats.P99), fmtDuration(noiseStats.P99))
	fmt.Println("╠═══════════════════╩════════════════╩════════════════════════╣")

	p50Diff := float64(noiseStats.P50-baselineStats.P50) / float64(baselineStats.P50) * 100
	fmt.Printf("║  P50 Impact: %+.1f%%", p50Diff)
	if p50Diff < 20 {
		fmt.Print("  ✅ ISOLATED")
	} else if p50Diff < 50 {
		fmt.Print("  ⚠️  MODERATE IMPACT")
	} else {
		fmt.Print("  ❌ NOISY NEIGHBOR DETECTED")
	}
	fmt.Println()
	fmt.Println("╚═════════════════════════════════════════════════════════════╝")
}

func fmtDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d.Microseconds()))
	}
	return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000)
}