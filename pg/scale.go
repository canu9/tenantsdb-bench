package pg

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"tenantsdb-bench/bench"

	"github.com/jackc/pgx/v5/pgxpool"
)

func buildTenantList() []string {
	var tenants []string
	for i := 1; i <= 10; i++ {
		tenants = append(tenants, fmt.Sprintf("bench_pg__bench%02d", i))
	}
	for i := 11; i <= 100; i++ {
		tenants = append(tenants, fmt.Sprintf("bench_pg__bench%03d", i))
	}
	return tenants
}

type tenantStats struct {
	Name    string
	Stats   bench.BenchStats
	Results []bench.QueryResult
}

func RunScale(proxyCfg bench.ConnConfig, params bench.BenchParams) {
	tenants := buildTenantList()
	concPerTenant := params.Concurrency / len(tenants)
	if concPerTenant < 1 {
		concPerTenant = 1
	}
	totalConc := concPerTenant * len(tenants)

	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  PostgreSQL Scale Benchmark (100 Tenants)")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Tenants:             %d\n", len(tenants))
	fmt.Printf("  Concurrency/tenant:  %d\n", concPerTenant)
	fmt.Printf("  Total concurrency:   %d\n", totalConc)
	if params.Duration > 0 {
		fmt.Printf("  Duration:            %s\n", params.Duration)
	} else {
		queriesPerTenant := params.Queries / len(tenants)
		if queriesPerTenant < 10 {
			queriesPerTenant = 10
		}
		fmt.Printf("  Queries/tenant:      %d\n", queriesPerTenant)
		fmt.Printf("  Total queries:       %d\n", queriesPerTenant*len(tenants))
	}
	fmt.Printf("  Workload:            80%% read / 20%% write\n\n")

	// ── Phase 1: Connect all tenants ──
	fmt.Println("[1/3] Connecting all tenants...")
	pools := make([]*pgxpool.Pool, len(tenants))
	var connectFailed int
	for i, t := range tenants {
		cfg := proxyCfg
		cfg.Database = t
		pool, err := Connect(cfg, "disable")
		if err != nil {
			fmt.Printf("  ✗ %s: %v\n", t, err)
			connectFailed++
			continue
		}
		pools[i] = pool
		if (i+1)%20 == 0 || i == len(tenants)-1 {
			fmt.Printf("  Connected: %d/%d\n", i+1-connectFailed, len(tenants))
		}
	}
	defer func() {
		for _, p := range pools {
			if p != nil {
				p.Close()
			}
		}
	}()
	if connectFailed > 0 {
		fmt.Printf("  ⚠ %d tenants failed to connect\n", connectFailed)
	}
	fmt.Printf("  ✓ %d tenants connected\n\n", len(tenants)-connectFailed)

	// ── Phase 2: Seed all tenants ──
	fmt.Println("[2/3] Seeding data (parallel)...")
	var seedWg sync.WaitGroup
	var seedFailed int
	var seedMu sync.Mutex
	for i, pool := range pools {
		if pool == nil {
			continue
		}
		seedWg.Add(1)
		go func(p *pgxpool.Pool, idx int) {
			defer seedWg.Done()
			if err := SeedData(p, params.SeedRows); err != nil {
				seedMu.Lock()
				seedFailed++
				seedMu.Unlock()
			}
		}(pool, i)
	}
	seedWg.Wait()
	if seedFailed > 0 {
		fmt.Printf("  ⚠ %d tenants failed to seed\n", seedFailed)
	}
	fmt.Println("  ✓ All tenants seeded\n")

	// ── Phase 3: Run scale benchmark ──
	fmt.Println("[3/3] Running scale benchmark...")
	fmt.Println()

	runOnce := func(run int) bench.BenchStats {
		if params.Duration > 0 {
			return scaleRunTimed(pools, tenants, params, concPerTenant, totalConc)
		}
		return scaleRunCount(pools, tenants, params, concPerTenant, totalConc)
	}

	if params.Runs > 1 {
		stats := bench.RunMultiple(params.Runs, "Scale (100 tenants)", runOnce)
		bench.PrintStats(stats)
	} else {
		stats := runOnce(0)
		bench.PrintStats(stats)
	}
}

func scaleRunCount(pools []*pgxpool.Pool, tenants []string, params bench.BenchParams, concPerTenant, totalConc int) bench.BenchStats {
	maxID := params.SeedRows
	queriesPerTenant := params.Queries / len(tenants)
	if queriesPerTenant < 10 {
		queriesPerTenant = 10
	}

	tResults := make([]tenantStats, len(tenants))
	for i, t := range tenants {
		tResults[i] = tenantStats{
			Name:    t,
			Results: make([]bench.QueryResult, queriesPerTenant),
		}
	}

	start := time.Now()
	var wg sync.WaitGroup

	for t := 0; t < len(tenants); t++ {
		pool := pools[t]
		if pool == nil {
			continue
		}

		for w := 0; w < concPerTenant; w++ {
			wg.Add(1)
			workerQueries := queriesPerTenant / concPerTenant
			workerOffset := w * workerQueries

			go func(tIdx int, p *pgxpool.Pool, offset, count int) {
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
						tResults[tIdx].Results[idx] = bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err}
					} else {
						id := rand.Intn(maxID) + 1
						delta := rand.Float64()*200 - 100
						_, err := p.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", delta, id)
						tResults[tIdx].Results[idx] = bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err}
					}
				}
			}(t, pool, workerOffset, workerQueries)
		}
	}
	wg.Wait()

	totalDuration := time.Since(start)
	return computeScaleStats(tResults, pools, tenants, totalDuration, totalConc)
}

func scaleRunTimed(pools []*pgxpool.Pool, tenants []string, params bench.BenchParams, concPerTenant, totalConc int) bench.BenchStats {
	maxID := params.SeedRows

	// Per-tenant result collection with per-tenant mutex
	type tenantCollector struct {
		mu      sync.Mutex
		results []bench.QueryResult
	}
	collectors := make([]tenantCollector, len(tenants))

	var stopped atomic.Bool
	start := time.Now()
	time.AfterFunc(params.Duration, func() { stopped.Store(true) })

	var wg sync.WaitGroup
	for t := 0; t < len(tenants); t++ {
		pool := pools[t]
		if pool == nil {
			continue
		}

		for w := 0; w < concPerTenant; w++ {
			wg.Add(1)
			go func(tIdx int, p *pgxpool.Pool) {
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
						err := p.QueryRow(ctx, "SELECT id, name, balance FROM accounts WHERE id = $1", id).Scan(&rID, &rName, &rBalance)
						local = append(local, bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err})
					} else {
						id := rand.Intn(maxID) + 1
						delta := rand.Float64()*200 - 100
						_, err := p.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", delta, id)
						local = append(local, bench.QueryResult{At: qStart, Duration: time.Since(qStart), Err: err})
					}
				}

				collectors[tIdx].mu.Lock()
				collectors[tIdx].results = append(collectors[tIdx].results, local...)
				collectors[tIdx].mu.Unlock()
			}(t, pool)
		}
	}
	wg.Wait()

	totalDuration := time.Since(start)

	// Convert collectors to tenantStats
	tResults := make([]tenantStats, len(tenants))
	for i, t := range tenants {
		tResults[i] = tenantStats{Name: t, Results: collectors[i].results}
	}

	return computeScaleStats(tResults, pools, tenants, totalDuration, totalConc)
}

func computeScaleStats(tResults []tenantStats, pools []*pgxpool.Pool, tenants []string, totalDuration time.Duration, totalConc int) bench.BenchStats {
	var allResults []bench.QueryResult
	var totalErrors int
	var tenantP50s []float64

	for i := range tResults {
		if pools[i] == nil {
			continue
		}
		tResults[i].Stats = bench.ComputeStats(tResults[i].Name, tResults[i].Results, totalDuration)
		allResults = append(allResults, tResults[i].Results...)
		totalErrors += tResults[i].Stats.Errors
		tenantP50s = append(tenantP50s, float64(tResults[i].Stats.LatencyP50.Microseconds()))
	}

	overall := bench.ComputeStats(
		fmt.Sprintf("Scale Test (%d tenants, %d total concurrent)", len(tenants), totalConc),
		allResults, totalDuration,
	)

	// ── Fairness analysis ──
	if len(tenantP50s) > 0 {
		sort.Float64s(tenantP50s)

		fastestP50 := time.Duration(tenantP50s[0]) * time.Microsecond
		slowestP50 := time.Duration(tenantP50s[len(tenantP50s)-1]) * time.Microsecond
		medianP50 := time.Duration(tenantP50s[len(tenantP50s)/2]) * time.Microsecond

		type ranked struct {
			name string
			p50  time.Duration
		}
		var ranking []ranked
		for i := range tResults {
			if pools[i] == nil {
				continue
			}
			ranking = append(ranking, ranked{tResults[i].Name, tResults[i].Stats.LatencyP50})
		}
		sort.Slice(ranking, func(i, j int) bool { return ranking[i].p50 > ranking[j].p50 })

		fairnessRatio := float64(slowestP50) / float64(fastestP50)

		fmt.Println()
		fmt.Println("╔═════════════════════════════════════════════════════════════╗")
		fmt.Println("║  SCALE TEST RESULTS (100 TENANTS)                          ║")
		fmt.Println("╠═════════════════════════════════════════════════════════════╣")
		fmt.Printf("║  Total Queries:     %-39d║\n", overall.Total)
		fmt.Printf("║  Total Errors:      %-39d║\n", totalErrors)
		fmt.Printf("║  Total Duration:    %-39s║\n", totalDuration.Round(time.Millisecond))
		fmt.Printf("║  Overall QPS:       %-39.1f║\n", overall.QPS)
		fmt.Printf("║  Overall p50:       %-39s║\n", bench.FmtDur(overall.LatencyP50))
		fmt.Printf("║  Overall p95:       %-39s║\n", bench.FmtDur(overall.LatencyP95))
		fmt.Printf("║  Overall p99:       %-39s║\n", bench.FmtDur(overall.LatencyP99))
		fmt.Println("╠═════════════════════════════════════════════════════════════╣")
		fmt.Println("║  TENANT FAIRNESS                                           ║")
		fmt.Println("╠═════════════════════════════════════════════════════════════╣")
		fmt.Printf("║  Fastest tenant p50:  %-37s║\n", bench.FmtDur(fastestP50))
		fmt.Printf("║  Median tenant p50:   %-37s║\n", bench.FmtDur(medianP50))
		fmt.Printf("║  Slowest tenant p50:  %-37s║\n", bench.FmtDur(slowestP50))
		fmt.Printf("║  Fairness ratio:      %-37s║\n", fmt.Sprintf("%.1fx (slowest/fastest)", fairnessRatio))
		fmt.Println("╠═════════════════════════════════════════════════════════════╣")
		fmt.Println("║  TOP 5 SLOWEST TENANTS                                     ║")
		fmt.Println("╠═════════════════════════════════════════════════════════════╣")
		for i := 0; i < 5 && i < len(ranking); i++ {
			short := ranking[i].name
			if len(short) > 20 {
				short = short[len(short)-20:]
			}
			fmt.Printf("║  #%d  %-20s  p50: %-23s║\n", i+1, short, bench.FmtDur(ranking[i].p50))
		}
		fmt.Println("╠═════════════════════════════════════════════════════════════╣")

		if fairnessRatio < 3.0 {
			fmt.Println("║  ✅ FAIR — all tenants within 3x of each other              ║")
		} else if fairnessRatio < 5.0 {
			fmt.Println("║  ⚠️  MODERATE — some tenants slower than others              ║")
		} else {
			fmt.Println("║  ❌ UNFAIR — significant latency spread between tenants      ║")
		}
		fmt.Println("╚═════════════════════════════════════════════════════════════╝")
	}

	return overall
}