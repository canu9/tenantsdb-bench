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

func RunIsolation(proxyCfg bench.ConnConfig, params bench.BenchParams) {
	victim := proxyCfg.Database
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
	victimPool, err := Connect(victimCfg, "disable")
	if err != nil {
		fmt.Printf("  ✗ Failed: %v\n", err)
		return
	}
	defer victimPool.Close()
	if err := SeedData(victimPool, params.SeedRows); err != nil {
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
		p, err := Connect(cfg, "disable")
		if err != nil {
			fmt.Printf("  ✗ %s failed: %v\n", t, err)
			return
		}
		defer p.Close()
		noisyPools[i] = p

		if err := SeedData(p, params.SeedRows); err != nil {
			fmt.Printf("  ✗ Seed %s failed: %v\n", t, err)
			return
		}
	}
	fmt.Println("  ✓ All noisy tenants ready")

	fmt.Println("\n[3/3] Running isolation test...")
	maxID := params.SeedRows
	victimConc := 5

	// ── Phase 1: Victim alone ──
	fmt.Println("\n── Phase 1: Victim alone (no noise) ──")
	baselineStats := RunQueries(victimPool, bench.BenchParams{
		Queries:     params.Queries,
		Concurrency: victimConc,
		Warmup:      params.Warmup,
		SeedRows:    params.SeedRows,
	}, "Victim ALONE")
	bench.PrintStats(baselineStats)

	// ── Phase 2: Victim under noise ──
	fmt.Println("\n── Phase 2: Starting noisy neighbors ──")
	fmt.Printf("  Launching %d noisy tenants (heavy writes)...\n", len(noisy))

	stopNoise := make(chan struct{})
	var noiseWg sync.WaitGroup

	for _, p := range noisyPools {
		for w := 0; w < 5; w++ {
			noiseWg.Add(1)
			go func(pool *pgxpool.Pool) {
				defer noiseWg.Done()
				ctx := context.Background()
				for {
					select {
					case <-stopNoise:
						return
					default:
						id := rand.Intn(maxID) + 1
						delta := rand.Float64()*200 - 100
						pool.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", delta, id)
					}
				}
			}(p)
		}
	}

	time.Sleep(2 * time.Second)
	fmt.Println("  ✓ Noise running (9 tenants × 5 concurrent = 45 writers)")

	fmt.Println("\n── Measuring victim under noise ──")
	noiseStats := RunQueries(victimPool, bench.BenchParams{
		Queries:     params.Queries,
		Concurrency: victimConc,
		Warmup:      params.Warmup,
		SeedRows:    params.SeedRows,
	}, "Victim UNDER NOISE")
	bench.PrintStats(noiseStats)

	close(stopNoise)
	noiseWg.Wait()

	bench.PrintIsolation(baselineStats, noiseStats)
}