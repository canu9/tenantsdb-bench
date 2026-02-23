package bench

import (
	"fmt"
	"time"
)

// RunMultiple executes runFn N times, checks steady-state, returns median.
// runFn receives the run index (0-based) and returns stats for that run.
func RunMultiple(runs int, label string, runFn func(run int) BenchStats) BenchStats {
	if runs <= 1 {
		return runFn(0)
	}

	fmt.Printf("\n╔═══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  %d-RUN BENCHMARK: %-38s║\n", runs, label)
	fmt.Printf("║  Methodology: median of %d runs, steady-state verified    ║\n", runs)
	fmt.Printf("╚═══════════════════════════════════════════════════════════╝\n")

	allRuns := make([]BenchStats, runs)

	for i := 0; i < runs; i++ {
		fmt.Printf("\n── Run %d/%d ──\n", i+1, runs)
		allRuns[i] = runFn(i)

		fmt.Printf("  Run %d: QPS=%.1f  p50=%s  p95=%s  errors=%d\n",
			i+1, allRuns[i].QPS,
			FmtDur(allRuns[i].LatencyP50),
			FmtDur(allRuns[i].LatencyP95),
			allRuns[i].Errors)

		// Cleanup pause between runs (not after last)
		if i < runs-1 {
			fmt.Print("  Cooling down (3s)...")
			time.Sleep(3 * time.Second)
			fmt.Println(" done")
		}
	}

	// Steady-state check
	steady, maxDev := SteadyState(allRuns, 0.05)
	fmt.Printf("\n── Steady-State Check ──\n")
	fmt.Printf("  Max QPS deviation: %.1f%%\n", maxDev*100)
	if steady {
		fmt.Println("  ✅ PASSED (within ±5%)")
	} else {
		fmt.Printf("  ⚠️  FAILED (%.1f%% > 5%%) — results still reported as median\n", maxDev*100)
	}

	// Pick median
	median := MedianStats(allRuns)
	median.Label = label + " (median of " + fmt.Sprintf("%d", runs) + " runs)"

	// Summary table
	fmt.Printf("\n╔═══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  ALL RUNS SUMMARY                                        ║\n")
	fmt.Printf("╠═════╦══════════╦══════════╦══════════╦═══════════════════╣\n")
	fmt.Printf("║ Run ║   QPS    ║   p50    ║   p95    ║ Errors            ║\n")
	fmt.Printf("╠═════╬══════════╬══════════╬══════════╬═══════════════════╣\n")
	for i, r := range allRuns {
		marker := "  "
		if r.LatencyP50 == median.LatencyP50 && r.QPS == median.QPS {
			marker = "→ "
		}
		fmt.Printf("║ %s%d  ║ %8.1f ║ %8s ║ %8s ║ %-17d ║\n",
			marker, i+1, r.QPS, FmtDur(r.LatencyP50), FmtDur(r.LatencyP95), r.Errors)
	}
	fmt.Printf("╚═════╩══════════╩══════════╩══════════╩═══════════════════╝\n")
	fmt.Println("  → = median (reported)")

	return median
}