package pg

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"tenantsdb-bench/bench"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(c bench.ConnConfig, sslmode string) (*pgxpool.Pool, error) {
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

func SeedData(pool *pgxpool.Pool, rows int) error {
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

// RunQueries runs a fixed number of queries (count-based mode).
func RunQueries(pool *pgxpool.Pool, params bench.BenchParams, label string) bench.BenchStats {
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
					err := pool.QueryRow(ctx, "SELECT id, name, balance FROM accounts WHERE id = $1", id).Scan(&rID, &rName, &rBalance)
					results[idx] = bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err}
				} else {
					id := rand.Intn(maxID) + 1
					delta := rand.Float64()*200 - 100
					_, err := pool.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", delta, id)
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
// Returns results collected during the duration window.
func RunQueriesTimed(pool *pgxpool.Pool, params bench.BenchParams, label string) bench.BenchStats {
	if params.Duration <= 0 {
		return RunQueries(pool, params, label)
	}

	ctx := context.Background()
	maxID := params.SeedRows

	// Warmup
	fmt.Printf("  Warming up (%d queries)...\n", params.Warmup)
	for i := 0; i < params.Warmup; i++ {
		id := rand.Intn(maxID) + 1
		pool.QueryRow(ctx, "SELECT id, name, balance FROM accounts WHERE id = $1", id).Scan(new(int), new(string), new(float64))
	}

	fmt.Printf("  Running for %s (%d concurrent)...\n", params.Duration, params.Concurrency)

	var mu sync.Mutex
	var results []bench.QueryResult
	var stopped atomic.Bool

	start := time.Now()

	// Stop signal after duration
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
					err := pool.QueryRow(ctx, "SELECT id, name, balance FROM accounts WHERE id = $1", id).Scan(&rID, &rName, &rBalance)
					local = append(local, bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err})
				} else {
					id := rand.Intn(maxID) + 1
					delta := rand.Float64()*200 - 100
					_, err := pool.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", delta, id)
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
func PickRunner(pool *pgxpool.Pool, params bench.BenchParams, label string) bench.BenchStats {
	if params.Duration > 0 {
		return RunQueriesTimed(pool, params, label)
	}
	return RunQueries(pool, params, label)
}