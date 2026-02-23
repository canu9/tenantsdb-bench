package bench

import (
	"fmt"
	"time"
)

func PrintStats(s BenchStats) {
	fmt.Printf("\n┌─────────────────────────────────────────┐\n")
	fmt.Printf("│  %-39s│\n", s.Label)
	fmt.Printf("├─────────────────────────────────────────┤\n")
	fmt.Printf("│  Queries:      %-24d│\n", s.Total)
	fmt.Printf("│  Errors:       %-24d│\n", s.Errors)
	fmt.Printf("│  Duration:     %-24s│\n", s.Duration.Round(time.Millisecond))
	fmt.Printf("│  QPS:          %-24.1f│\n", s.QPS)
	fmt.Printf("├─────────────────────────────────────────┤\n")
	fmt.Printf("│  Latency avg:  %-24s│\n", FmtDur(s.LatencyAvg))
	fmt.Printf("│  Latency min:  %-24s│\n", FmtDur(s.LatencyMin))
	fmt.Printf("│  Latency max:  %-24s│\n", FmtDur(s.LatencyMax))
	fmt.Printf("│  Latency p50:  %-24s│\n", FmtDur(s.LatencyP50))
	fmt.Printf("│  Latency p75:  %-24s│\n", FmtDur(s.LatencyP75))
	fmt.Printf("│  Latency p90:  %-24s│\n", FmtDur(s.LatencyP90))
	fmt.Printf("│  Latency p95:  %-24s│\n", FmtDur(s.LatencyP95))
	fmt.Printf("│  Latency p99:  %-24s│\n", FmtDur(s.LatencyP99))
	fmt.Printf("└─────────────────────────────────────────┘\n")
}

func PrintComparison(proxy, direct BenchStats) {
	overhead := proxy.LatencyP50 - direct.LatencyP50
	overheadPct := float64(overhead) / float64(direct.LatencyP50) * 100
	qpsDrop := (direct.QPS - proxy.QPS) / direct.QPS * 100

	fmt.Printf("\n╔═════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  PROXY OVERHEAD COMPARISON                                 ║\n")
	fmt.Printf("╠═══════════════════╦════════════════╦════════════════════════╣\n")
	fmt.Printf("║  Metric           ║  Direct        ║  Through Proxy         ║\n")
	fmt.Printf("╠═══════════════════╬════════════════╬════════════════════════╣\n")
	fmt.Printf("║  QPS              ║  %-13.1f ║  %-21.1f ║\n", direct.QPS, proxy.QPS)
	fmt.Printf("║  Latency avg      ║  %-13s ║  %-21s ║\n", FmtDur(direct.LatencyAvg), FmtDur(proxy.LatencyAvg))
	fmt.Printf("║  Latency p50      ║  %-13s ║  %-21s ║\n", FmtDur(direct.LatencyP50), FmtDur(proxy.LatencyP50))
	fmt.Printf("║  Latency p95      ║  %-13s ║  %-21s ║\n", FmtDur(direct.LatencyP95), FmtDur(proxy.LatencyP95))
	fmt.Printf("║  Latency p99      ║  %-13s ║  %-21s ║\n", FmtDur(direct.LatencyP99), FmtDur(proxy.LatencyP99))
	fmt.Printf("╠═══════════════════╩════════════════╩════════════════════════╣\n")
	fmt.Printf("║  Proxy Overhead (p50):  %-35s ║\n", fmt.Sprintf("%s (%.1f%%)", FmtDur(overhead), overheadPct))
	fmt.Printf("║  QPS Drop:              %-35s ║\n", fmt.Sprintf("%.1f%%", qpsDrop))
	fmt.Printf("╚═════════════════════════════════════════════════════════════╝\n")
}

func PrintIsolation(baseline, noise BenchStats) {
	fmt.Println()
	fmt.Println("╔═════════════════════════════════════════════════════════════╗")
	fmt.Println("║  NOISY NEIGHBOR ISOLATION RESULTS                          ║")
	fmt.Println("╠═══════════════════╦════════════════╦════════════════════════╣")
	fmt.Println("║  Metric           ║  Alone         ║  Under Noise           ║")
	fmt.Println("╠═══════════════════╬════════════════╬════════════════════════╣")
	fmt.Printf("║  QPS              ║  %-13.1f ║  %-23.1f║\n", baseline.QPS, noise.QPS)
	fmt.Printf("║  Latency avg      ║  %-13s ║  %-23s║\n", FmtDur(baseline.LatencyAvg), FmtDur(noise.LatencyAvg))
	fmt.Printf("║  Latency p50      ║  %-13s ║  %-23s║\n", FmtDur(baseline.LatencyP50), FmtDur(noise.LatencyP50))
	fmt.Printf("║  Latency p95      ║  %-13s ║  %-23s║\n", FmtDur(baseline.LatencyP95), FmtDur(noise.LatencyP95))
	fmt.Printf("║  Latency p99      ║  %-13s ║  %-23s║\n", FmtDur(baseline.LatencyP99), FmtDur(noise.LatencyP99))
	fmt.Println("╠═══════════════════╩════════════════╩════════════════════════╣")

	p50Diff := float64(noise.LatencyP50-baseline.LatencyP50) / float64(baseline.LatencyP50) * 100
	fmt.Printf("║  P50 Impact: %+.1f%%", p50Diff)
	if p50Diff < 20 {
		fmt.Print("  ✅ ISOLATED")
	} else if p50Diff < 50 {
		fmt.Print("  ⚠️  MODERATE IMPACT")
	} else {
		fmt.Print("  ❌ NOISY NEIGHBOR DETECTED")
	}
	fmt.Println()
	fmt.Println("╚═════════════════════════════════════════════════════════════╝")
}

func FmtDur(d time.Duration) string {
	us := float64(d.Microseconds())
	if us < 1000 {
		return fmt.Sprintf("%.0fµs", us)
	}
	return fmt.Sprintf("%.2fms", us/1000)
}