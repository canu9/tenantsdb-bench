package my

import (
	"fmt"

	"tenantsdb-bench/bench"
)

func RunOverhead(proxyCfg, directCfg bench.ConnConfig, params bench.BenchParams) {
	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  MySQL Proxy Overhead Benchmark")
	fmt.Println("═══════════════════════════════════════════")
	if params.Duration > 0 {
		fmt.Printf("  Duration: %s | Concurrency: %d | Workload: 80%% read / 20%% write\n\n", params.Duration, params.Concurrency)
	} else {
		fmt.Printf("  Queries: %d | Concurrency: %d | Workload: 80%% read / 20%% write\n\n", params.Queries, params.Concurrency)
	}

	// Connect direct
	fmt.Println("[1/4] Connecting directly to MySQL...")
	directDB, err := Connect(directCfg)
	if err != nil {
		fmt.Printf("  ✗ Direct connection failed: %v\n", err)
		return
	}
	defer directDB.Close()
	fmt.Println("  ✓ Connected")

	// Seed data direct
	fmt.Println("\n[2/4] Seeding test data (direct)...")
	if err := SeedData(directDB, params.SeedRows); err != nil {
		fmt.Printf("  ✗ Seed failed: %v\n", err)
		return
	}
	fmt.Println("  ✓ Data ready")

	// Connect proxy
	fmt.Println("\n[3/4] Connecting through TenantsDB proxy...")
	proxyDB, err := Connect(proxyCfg)
	if err != nil {
		fmt.Printf("  ✗ Proxy connection failed: %v\n", err)
		return
	}
	defer proxyDB.Close()
	fmt.Println("  ✓ Connected")

	// Run benchmarks
	fmt.Println("\n[4/4] Running benchmarks...")

	if params.Runs > 1 {
		directStats := bench.RunMultiple(params.Runs, "Direct MySQL", func(run int) bench.BenchStats {
			return PickRunner(directDB, params, "Direct MySQL")
		})
		bench.PrintStats(directStats)

		proxyStats := bench.RunMultiple(params.Runs, "Through TenantsDB Proxy", func(run int) bench.BenchStats {
			return PickRunner(proxyDB, params, "Through TenantsDB Proxy")
		})
		bench.PrintStats(proxyStats)

		bench.PrintComparison(proxyStats, directStats)
	} else {
		fmt.Println("\n── Direct MySQL ──")
		directStats := PickRunner(directDB, params, "Direct MySQL")
		bench.PrintStats(directStats)

		fmt.Println("\n── Through TenantsDB Proxy ──")
		proxyStats := PickRunner(proxyDB, params, "Through TenantsDB Proxy")
		bench.PrintStats(proxyStats)

		bench.PrintComparison(proxyStats, directStats)
	}
}

func RunThroughput(proxyCfg bench.ConnConfig, params bench.BenchParams) {
	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  MySQL Throughput Benchmark")
	fmt.Println("═══════════════════════════════════════════")
	if params.Duration > 0 {
		fmt.Printf("  Duration: %s | Concurrency: %d\n\n", params.Duration, params.Concurrency)
	} else {
		fmt.Printf("  Queries: %d | Concurrency: %d\n\n", params.Queries, params.Concurrency)
	}

	fmt.Println("[1/3] Connecting through TenantsDB proxy...")
	db, err := Connect(proxyCfg)
	if err != nil {
		fmt.Printf("  ✗ Connection failed: %v\n", err)
		return
	}
	defer db.Close()
	fmt.Println("  ✓ Connected")

	fmt.Println("\n[2/3] Seeding test data...")
	if err := SeedData(db, params.SeedRows); err != nil {
		fmt.Printf("  ✗ Seed failed: %v\n", err)
		return
	}
	fmt.Println("  ✓ Data ready")

	fmt.Println("\n[3/3] Running benchmark...")

	if params.Runs > 1 {
		stats := bench.RunMultiple(params.Runs, "MySQL Throughput (via Proxy)", func(run int) bench.BenchStats {
			return PickRunner(db, params, "MySQL Throughput (via Proxy)")
		})
		bench.PrintStats(stats)
	} else {
		stats := PickRunner(db, params, "MySQL Throughput (via Proxy)")
		bench.PrintStats(stats)
	}
}