package my

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"tenantsdb-bench/bench"
)

func RunIsolation(proxyCfg bench.ConnConfig, params bench.BenchParams) {
	victim := proxyCfg.Database
	noisy := []string{
		"bench_mysql__bench02", "bench_mysql__bench03", "bench_mysql__bench04",
		"bench_mysql__bench05", "bench_mysql__bench06", "bench_mysql__bench07",
		"bench_mysql__bench08", "bench_mysql__bench09", "bench_mysql__bench10",
	}

	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  MySQL Noisy Neighbor Isolation Test")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Victim tenant: %s\n", victim)
	fmt.Printf("  Noisy tenants: %d (each hammering with writes)\n\n", len(noisy))

	// Connect victim
	fmt.Println("[1/3] Connecting victim tenant...")
	victimCfg := proxyCfg
	victimCfg.Database = victim
	victimDB, err := Connect(victimCfg)
	if err != nil {
		fmt.Printf("  ✗ Failed: %v\n", err)
		return
	}
	defer victimDB.Close()
	if err := SeedData(victimDB, params.SeedRows); err != nil {
		fmt.Printf("  ✗ Seed failed: %v\n", err)
		return
	}
	fmt.Println("  ✓ Victim ready")

	// Connect noisy tenants
	fmt.Println("\n[2/3] Connecting noisy tenants...")
	noisyDBs := make([]*sql.DB, len(noisy))
	for i, t := range noisy {
		cfg := proxyCfg
		cfg.Database = t
		db, err := Connect(cfg)
		if err != nil {
			fmt.Printf("  ✗ %s failed: %v\n", t, err)
			return
		}
		defer db.Close()
		noisyDBs[i] = db

		if err := SeedData(db, params.SeedRows); err != nil {
			fmt.Printf("  ✗ Seed %s failed: %v\n", t, err)
			return
		}
	}
	fmt.Println("  ✓ All noisy tenants ready")

	fmt.Println("\n[3/3] Running isolation test...")
	maxID := params.SeedRows
	victimConc := 5

	victimParams := bench.BenchParams{
		Queries:     params.Queries,
		Concurrency: victimConc,
		Warmup:      params.Warmup,
		SeedRows:    params.SeedRows,
		Duration:    params.Duration,
	}

	// ── Phase 1: Victim alone ──
	fmt.Println("\n── Phase 1: Victim alone (no noise) ──")
	var baselineStats bench.BenchStats
	if params.Runs > 1 {
		baselineStats = bench.RunMultiple(params.Runs, "Victim ALONE", func(run int) bench.BenchStats {
			return PickRunner(victimDB, victimParams, "Victim ALONE")
		})
	} else {
		baselineStats = PickRunner(victimDB, victimParams, "Victim ALONE")
	}
	bench.PrintStats(baselineStats)

	// ── Phase 2: Victim under noise ──
	fmt.Println("\n── Phase 2: Starting noisy neighbors ──")
	fmt.Printf("  Launching %d noisy tenants (heavy writes)...\n", len(noisy))

	stopNoise := make(chan struct{})
	var noiseWg sync.WaitGroup

	for _, db := range noisyDBs {
		for w := 0; w < 5; w++ {
			noiseWg.Add(1)
			go func(d *sql.DB) {
				defer noiseWg.Done()
				ctx := context.Background()
				for {
					select {
					case <-stopNoise:
						return
					default:
						id := rand.Intn(maxID) + 1
						delta := rand.Float64()*200 - 100
						d.ExecContext(ctx, "UPDATE accounts SET balance = balance + ? WHERE id = ?", delta, id)
					}
				}
			}(db)
		}
	}

	time.Sleep(2 * time.Second)
	fmt.Println("  ✓ Noise running (9 tenants × 5 concurrent = 45 writers)")

	fmt.Println("\n── Measuring victim under noise ──")
	var noiseStats bench.BenchStats
	if params.Runs > 1 {
		noiseStats = bench.RunMultiple(params.Runs, "Victim UNDER NOISE", func(run int) bench.BenchStats {
			return PickRunner(victimDB, victimParams, "Victim UNDER NOISE")
		})
	} else {
		noiseStats = PickRunner(victimDB, victimParams, "Victim UNDER NOISE")
	}
	bench.PrintStats(noiseStats)

	close(stopNoise)
	noiseWg.Wait()

	bench.PrintIsolation(baselineStats, noiseStats)
}