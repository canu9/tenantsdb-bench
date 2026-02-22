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