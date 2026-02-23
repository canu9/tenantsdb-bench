package main

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func buildTenantList() []string {
	var tenants []string
	// bench01-bench10 (2-digit, created first)
	for i := 1; i <= 10; i++ {
		tenants = append(tenants, fmt.Sprintf("bench_pg__bench%02d", i))
	}
	// bench011-bench100 (3-digit, created second)
	for i := 11; i <= 100; i++ {
		tenants = append(tenants, fmt.Sprintf("bench_pg__bench%03d", i))
	}
	return tenants
}

type TenantStats struct {
	Name    string
	Stats   BenchStats
	Results []QueryResult
}

func RunPostgresScale(proxyCfg ConnConfig, params BenchParams) {
	tenants := buildTenantList()
	concPerTenant := params.Concurrency / len(tenants)
	if concPerTenant < 1 {
		concPerTenant = 1
	}
	totalConc := concPerTenant * len(tenants)
	queriesPerTenant := params.Queries / len(tenants)
	if queriesPerTenant < 10 {
		queriesPerTenant = 10
	}

	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  PostgreSQL Scale Benchmark (100 Tenants)")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Tenants:             %d\n", len(tenants))
	fmt.Printf("  Concurrency/tenant:  %d\n", concPerTenant)
	fmt.Printf("  Total concurrency:   %d\n", totalConc)
	fmt.Printf("  Queries/tenant:      %d\n", queriesPerTenant)
	fmt.Printf("  Total queries:       %d\n", queriesPerTenant*len(tenants))
	fmt.Printf("  Workload:            80%% read / 20%% write\n\n")

	// ── Phase 1: Connect all tenants ──
	fmt.Println("[1/3] Connecting all tenants...")
	pools := make([]*pgxpool.Pool, len(tenants))
	var connectFailed int
	for i, t := range tenants {
		cfg := proxyCfg
		cfg.Database = t
		pool, err := pgConnect(cfg, "disable")
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
			if err := pgSeedData(p, params.SeedRows); err != nil {
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

	maxID := params.SeedRows
	tenantResults := make([]TenantStats, len(tenants))

	// Initialize per-tenant result slices
	for i, t := range tenants {
		tenantResults[i] = TenantStats{
			Name:    t,
			Results: make([]QueryResult, queriesPerTenant),
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
						tenantResults[tIdx].Results[idx] = QueryResult{Duration: time.Since(qStart), Err: err}
					} else {
						id := rand.Intn(maxID) + 1
						delta := rand.Float64()*200 - 100
						_, err := p.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", delta, id)
						tenantResults[tIdx].Results[idx] = QueryResult{Duration: time.Since(qStart), Err: err}
					}
				}
			}(t, pool, workerOffset, workerQueries)
		}
	}
	wg.Wait()

	totalDuration := time.Since(start)

	// ── Compute per-tenant stats ──
	var allResults []QueryResult
	var totalErrors int
	var tenantP50s []float64

	for i := range tenantResults {
		if pools[i] == nil {
			continue
		}
		tenantResults[i].Stats = ComputeStats(tenantResults[i].Name, tenantResults[i].Results, totalDuration)
		allResults = append(allResults, tenantResults[i].Results...)
		totalErrors += tenantResults[i].Stats.Errors
		tenantP50s = append(tenantP50s, float64(tenantResults[i].Stats.LatencyP50.Microseconds()))
	}

	// Overall stats
	overall := ComputeStats(
		fmt.Sprintf("Scale Test (%d tenants, %d total concurrent)", len(tenants), totalConc),
		allResults, totalDuration,
	)

	// ── Print overall results ──
	PrintStats(overall)

	// ── Fairness analysis ──
	sort.Float64s(tenantP50s)

	fastestP50 := time.Duration(tenantP50s[0]) * time.Microsecond
	slowestP50 := time.Duration(tenantP50s[len(tenantP50s)-1]) * time.Microsecond
	medianP50 := time.Duration(tenantP50s[len(tenantP50s)/2]) * time.Microsecond

	// Find top 5 slowest tenants
	type ranked struct {
		name string
		p50  time.Duration
	}
	var ranking []ranked
	for i := range tenantResults {
		if pools[i] == nil {
			continue
		}
		ranking = append(ranking, ranked{tenantResults[i].Name, tenantResults[i].Stats.LatencyP50})
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
	fmt.Printf("║  Overall p50:       %-39s║\n", fmtDuration(overall.LatencyP50))
	fmt.Printf("║  Overall p95:       %-39s║\n", fmtDuration(overall.LatencyP95))
	fmt.Printf("║  Overall p99:       %-39s║\n", fmtDuration(overall.LatencyP99))
	fmt.Println("╠═════════════════════════════════════════════════════════════╣")
	fmt.Println("║  TENANT FAIRNESS                                           ║")
	fmt.Println("╠═════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Fastest tenant p50:  %-37s║\n", fmtDuration(fastestP50))
	fmt.Printf("║  Median tenant p50:   %-37s║\n", fmtDuration(medianP50))
	fmt.Printf("║  Slowest tenant p50:  %-37s║\n", fmtDuration(slowestP50))
	fmt.Printf("║  Fairness ratio:      %-37s║\n", fmt.Sprintf("%.1fx (slowest/fastest)", fairnessRatio))
	fmt.Println("╠═════════════════════════════════════════════════════════════╣")
	fmt.Println("║  TOP 5 SLOWEST TENANTS                                     ║")
	fmt.Println("╠═════════════════════════════════════════════════════════════╣")
	for i := 0; i < 5 && i < len(ranking); i++ {
		short := ranking[i].name
		if len(short) > 20 {
			short = short[len(short)-20:]
		}
		fmt.Printf("║  #%d  %-20s  p50: %-23s║\n", i+1, short, fmtDuration(ranking[i].p50))
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