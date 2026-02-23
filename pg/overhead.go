package pg

import (
	"fmt"

	"tenantsdb-bench/bench"
)

func RunOverhead(proxyCfg, directCfg bench.ConnConfig, params bench.BenchParams) {
	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  PostgreSQL Proxy Overhead Benchmark")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Queries: %d | Concurrency: %d | Workload: 80%% read / 20%% write\n\n", params.Queries, params.Concurrency)

	// Connect direct
	fmt.Println("[1/4] Connecting directly to PostgreSQL...")
	directPool, err := Connect(directCfg, "disable")
	if err != nil {
		fmt.Printf("  ✗ Direct connection failed: %v\n", err)
		return
	}
	defer directPool.Close()
	fmt.Println("  ✓ Connected")

	// Seed data direct
	fmt.Println("\n[2/4] Seeding test data (direct)...")
	if err := SeedData(directPool, params.SeedRows); err != nil {
		fmt.Printf("  ✗ Seed failed: %v\n", err)
		return
	}
	fmt.Println("  ✓ Data ready")

	// Connect proxy
	fmt.Println("\n[3/4] Connecting through TenantsDB proxy...")
	proxyPool, err := Connect(proxyCfg, "disable")
	if err != nil {
		fmt.Printf("  ✗ Proxy connection failed: %v\n", err)
		return
	}
	defer proxyPool.Close()
	fmt.Println("  ✓ Connected")

	// Run benchmarks
	fmt.Println("\n[4/4] Running benchmarks...")
	fmt.Println("\n── Direct PostgreSQL ──")
	directStats := RunQueries(directPool, params, "Direct PostgreSQL")
	bench.PrintStats(directStats)

	fmt.Println("\n── Through TenantsDB Proxy ──")
	proxyStats := RunQueries(proxyPool, params, "Through TenantsDB Proxy")
	bench.PrintStats(proxyStats)

	// Comparison
	bench.PrintComparison(proxyStats, directStats)
}

func RunThroughput(proxyCfg bench.ConnConfig, params bench.BenchParams) {
	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  PostgreSQL Throughput Benchmark")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Queries: %d | Concurrency: %d\n\n", params.Queries, params.Concurrency)

	fmt.Println("[1/3] Connecting through TenantsDB proxy...")
	pool, err := Connect(proxyCfg, "disable")
	if err != nil {
		fmt.Printf("  ✗ Connection failed: %v\n", err)
		return
	}
	defer pool.Close()
	fmt.Println("  ✓ Connected")

	fmt.Println("\n[2/3] Seeding test data...")
	if err := SeedData(pool, params.SeedRows); err != nil {
		fmt.Printf("  ✗ Seed failed: %v\n", err)
		return
	}
	fmt.Println("  ✓ Data ready")

	fmt.Println("\n[3/3] Running benchmark...")
	stats := RunQueries(pool, params, "PostgreSQL Throughput (via Proxy)")
	bench.PrintStats(stats)
}