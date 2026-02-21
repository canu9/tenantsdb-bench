package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func pgDSN(c ConnConfig, sslmode string) string {
	if sslmode == "" {
		sslmode = "disable"
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Database, sslmode)
}

func pgConnect(c ConnConfig, sslmode string) (*sql.DB, error) {
	db, err := sql.Open("postgres", pgDSN(c, sslmode))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)
	return db, db.Ping()
}

func pgSeedData(db *sql.DB, rows int) error {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM accounts").Scan(&count)
	if err != nil {
		return fmt.Errorf("seed check: %w", err)
	}
	if count >= rows {
		fmt.Printf("  Data already seeded (%d rows)\n", count)
		return nil
	}

	fmt.Printf("  Seeding %d rows...\n", rows)
	_, err = db.Exec(fmt.Sprintf(`
		INSERT INTO accounts (name, balance)
		SELECT 'user_' || i, (random() * 10000)::decimal(15,2)
		FROM generate_series(1, %d) i
		ON CONFLICT DO NOTHING
	`, rows))
	return err
}

func pgRunQueries(db *sql.DB, params BenchParams, label string) BenchStats {
	maxID := params.SeedRows

	// Warmup
	fmt.Printf("  Warming up (%d queries)...\n", params.Warmup)
	for i := 0; i < params.Warmup; i++ {
		id := rand.Intn(maxID) + 1
		db.QueryRow("SELECT id, name, balance FROM accounts WHERE id = $1", id).Scan(new(int), new(string), new(float64))
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
					err := db.QueryRow("SELECT id, name, balance FROM accounts WHERE id = $1", id).Scan(&rID, &rName, &rBalance)
					results[idx] = QueryResult{Duration: time.Since(qStart), Err: err}
				} else {
					id := rand.Intn(maxID) + 1
					delta := rand.Float64()*200 - 100
					_, err := db.Exec("UPDATE accounts SET balance = balance + $1 WHERE id = $2", delta, id)
					results[idx] = QueryResult{Duration: time.Since(qStart), Err: err}
				}
			}
		}(w)
	}
	wg.Wait()

	totalDuration := time.Since(start)
	return ComputeStats(label, results, totalDuration)
}

func RunPostgresOverhead(proxyCfg, directCfg ConnConfig, params BenchParams) {
	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  PostgreSQL Proxy Overhead Benchmark")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Queries: %d | Concurrency: %d | Workload: 80%% read / 20%% write\n\n", params.Queries, params.Concurrency)

	// Connect direct
	fmt.Println("[1/4] Connecting directly to PostgreSQL...")
	directDB, err := pgConnect(directCfg, "disable")
	if err != nil {
		fmt.Printf("  ✗ Direct connection failed: %v\n", err)
		return
	}
	defer directDB.Close()
	fmt.Println("  ✓ Connected")

	// Seed data direct
	fmt.Println("\n[2/4] Seeding test data (direct)...")
	if err := pgSeedData(directDB, params.SeedRows); err != nil {
		fmt.Printf("  ✗ Seed failed: %v\n", err)
		return
	}
	fmt.Println("  ✓ Data ready")

	// Connect proxy
	fmt.Println("\n[3/4] Connecting through TenantsDB proxy...")
	proxyDB, err := pgConnect(proxyCfg, "require")
	if err != nil {
			fmt.Printf("  ✗ Proxy connection failed: %v\n", err)
			return
		
	}
	defer proxyDB.Close()
	fmt.Println("  ✓ Connected")

	// Run benchmarks
	fmt.Println("\n[4/4] Running benchmarks...")
	fmt.Println("\n── Direct PostgreSQL ──")
	directStats := pgRunQueries(directDB, params, "Direct PostgreSQL")
	PrintStats(directStats)

	fmt.Println("\n── Through TenantsDB Proxy ──")
	proxyStats := pgRunQueries(proxyDB, params, "Through TenantsDB Proxy")
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
	db, err := pgConnect(proxyCfg, "require")
	if err != nil {
			fmt.Printf("  ✗ Connection failed: %v\n", err)
			return
		
	}
	defer db.Close()
	fmt.Println("  ✓ Connected")

	fmt.Println("\n[2/3] Seeding test data...")
	if err := pgSeedData(db, params.SeedRows); err != nil {
		fmt.Printf("  ✗ Seed failed: %v\n", err)
		return
	}
	fmt.Println("  ✓ Data ready")

	fmt.Println("\n[3/3] Running benchmark...")
	stats := pgRunQueries(db, params, "PostgreSQL Throughput (via Proxy)")
	PrintStats(stats)
}