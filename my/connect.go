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

	_ "github.com/go-sql-driver/mysql"
)

func Connect(c bench.ConnConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&interpolateParams=true&allowCleartextPasswords=true&timeout=30s",
		c.User, c.Password, c.Host, c.Port, c.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func SeedData(db *sql.DB, rows int) error {
	ctx := context.Background()

	// Check if table already exists and seeded
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts").Scan(&count); err == nil {
		if count >= rows {
			fmt.Printf("  Data already seeded (%d rows)\n", count)
			return nil
		}
		fmt.Printf("  Table exists with %d rows, seeding more...\n", count)
	}

	// Create table if not exists (only works on direct connections, blocked by proxy DDL guard)
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS accounts (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			balance DECIMAL(15,2) NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts").Scan(&count)
	if err != nil {
		return fmt.Errorf("seed check: %w", err)
	}
	if count >= rows {
		fmt.Printf("  Data already seeded (%d rows)\n", count)
		return nil
	}

	fmt.Printf("  Seeding %d rows...\n", rows)

	// Batch insert 500 at a time
	batchSize := 500
	for i := 0; i < rows; i += batchSize {
		end := i + batchSize
		if end > rows {
			end = rows
		}

		query := "INSERT INTO accounts (name, balance) VALUES "
		vals := make([]interface{}, 0, (end-i)*2)
		for j := i; j < end; j++ {
			if j > i {
				query += ","
			}
			query += "(?,?)"
			vals = append(vals, fmt.Sprintf("user_%d", j+1), rand.Float64()*10000)
		}

		if _, err := db.ExecContext(ctx, query, vals...); err != nil {
			return fmt.Errorf("seed batch at %d: %w", i, err)
		}
	}
	return nil
}

// RunQueries runs a fixed number of queries (count-based mode).
func RunQueries(db *sql.DB, params bench.BenchParams, label string) bench.BenchStats {
	ctx := context.Background()
	maxID := params.SeedRows

	// Warmup
	fmt.Printf("  Warming up (%d queries)...\n", params.Warmup)
	for i := 0; i < params.Warmup; i++ {
		id := rand.Intn(maxID) + 1
		db.QueryRowContext(ctx, "SELECT id, name, balance FROM accounts WHERE id = ?", id).Scan(new(int), new(string), new(float64))
	}

	// Benchmark
	fmt.Printf("  Running %d queries (%d concurrent)...\n", params.Queries, params.Concurrency)

	results := make([]bench.QueryResult, params.Queries)
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

				if rand.Intn(100) < 80 {
					id := rand.Intn(maxID) + 1
					var rID int
					var rName string
					var rBalance float64
					err := db.QueryRowContext(ctx, "SELECT id, name, balance FROM accounts WHERE id = ?", id).Scan(&rID, &rName, &rBalance)
					results[idx] = bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err}
				} else {
					id := rand.Intn(maxID) + 1
					delta := rand.Float64()*200 - 100
					_, err := db.ExecContext(ctx, "UPDATE accounts SET balance = balance + ? WHERE id = ?", delta, id)
					results[idx] = bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err}
				}
			}
		}(w)
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

	return bench.ComputeStats(label, results, totalDuration)
}

// RunQueriesTimed runs queries for a fixed duration (time-based mode).
func RunQueriesTimed(db *sql.DB, params bench.BenchParams, label string) bench.BenchStats {
	if params.Duration <= 0 {
		return RunQueries(db, params, label)
	}

	ctx := context.Background()
	maxID := params.SeedRows

	// Warmup
	fmt.Printf("  Warming up (%d queries)...\n", params.Warmup)
	for i := 0; i < params.Warmup; i++ {
		id := rand.Intn(maxID) + 1
		db.QueryRowContext(ctx, "SELECT id, name, balance FROM accounts WHERE id = ?", id).Scan(new(int), new(string), new(float64))
	}

	fmt.Printf("  Running for %s (%d concurrent)...\n", params.Duration, params.Concurrency)

	var mu sync.Mutex
	var results []bench.QueryResult
	var stopped atomic.Bool

	start := time.Now()
	time.AfterFunc(params.Duration, func() { stopped.Store(true) })

	var wg sync.WaitGroup
	for w := 0; w < params.Concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var local []bench.QueryResult

			for !stopped.Load() {
				qStart := time.Now()

				if rand.Intn(100) < 80 {
					id := rand.Intn(maxID) + 1
					var rID int
					var rName string
					var rBalance float64
					err := db.QueryRowContext(ctx, "SELECT id, name, balance FROM accounts WHERE id = ?", id).Scan(&rID, &rName, &rBalance)
					local = append(local, bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err})
				} else {
					id := rand.Intn(maxID) + 1
					delta := rand.Float64()*200 - 100
					_, err := db.ExecContext(ctx, "UPDATE accounts SET balance = balance + ? WHERE id = ?", delta, id)
					local = append(local, bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err})
				}
			}

			mu.Lock()
			results = append(results, local...)
			mu.Unlock()
		}()
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

	return bench.ComputeStats(label, results, totalDuration)
}

// PickRunner returns the right runner based on params.Duration.
func PickRunner(db *sql.DB, params bench.BenchParams, label string) bench.BenchStats {
	if params.Duration > 0 {
		return RunQueriesTimed(db, params, label)
	}
	return RunQueries(db, params, label)
}