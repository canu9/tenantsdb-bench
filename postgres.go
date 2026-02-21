package main

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type ConnConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

type BenchParams struct {
	Queries     int
	Concurrency int
	Warmup      int
	SeedRows    int
}

type QueryResult struct {
	Duration time.Duration
	Err      error
}

type BenchStats struct {
	Label       string
	Total       int
	Errors      int
	Duration    time.Duration
	QPS         float64
	LatencyAvg  time.Duration
	LatencyMin  time.Duration
	LatencyMax  time.Duration
	LatencyP50  time.Duration
	LatencyP95  time.Duration
	LatencyP99  time.Duration
}

func ComputeStats(label string, results []QueryResult, totalDuration time.Duration) BenchStats {
	stats := BenchStats{Label: label, Total: len(results), Duration: totalDuration}

	var durations []time.Duration
	for _, r := range results {
		if r.Err != nil {
			stats.Errors++
			continue
		}
		durations = append(durations, r.Duration)
	}

	if len(durations) == 0 {
		return stats
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	var sum time.Duration
	for _, d := range durations {
		sum += d
	}

	stats.LatencyAvg = sum / time.Duration(len(durations))
	stats.LatencyMin = durations[0]
	stats.LatencyMax = durations[len(durations)-1]
	stats.LatencyP50 = percentile(durations, 50)
	stats.LatencyP95 = percentile(durations, 95)
	stats.LatencyP99 = percentile(durations, 99)
	stats.QPS = float64(len(durations)) / totalDuration.Seconds()

	return stats
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func PrintStats(s BenchStats) {
	fmt.Printf("\n┌─────────────────────────────────────────┐\n")
	fmt.Printf("│  %-39s│\n", s.Label)
	fmt.Printf("├─────────────────────────────────────────┤\n")
	fmt.Printf("│  Queries:      %-24d│\n", s.Total)
	fmt.Printf("│  Errors:       %-24d│\n", s.Errors)
	fmt.Printf("│  Duration:     %-24s│\n", s.Duration.Round(time.Millisecond))
	fmt.Printf("│  QPS:          %-24.1f│\n", s.QPS)
	fmt.Printf("├─────────────────────────────────────────┤\n")
	fmt.Printf("│  Latency avg:  %-24s│\n", fmtDur(s.LatencyAvg))
	fmt.Printf("│  Latency min:  %-24s│\n", fmtDur(s.LatencyMin))
	fmt.Printf("│  Latency max:  %-24s│\n", fmtDur(s.LatencyMax))
	fmt.Printf("│  Latency p50:  %-24s│\n", fmtDur(s.LatencyP50))
	fmt.Printf("│  Latency p95:  %-24s│\n", fmtDur(s.LatencyP95))
	fmt.Printf("│  Latency p99:  %-24s│\n", fmtDur(s.LatencyP99))
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
	fmt.Printf("║  Latency avg      ║  %-13s ║  %-21s ║\n", fmtDur(direct.LatencyAvg), fmtDur(proxy.LatencyAvg))
	fmt.Printf("║  Latency p50      ║  %-13s ║  %-21s ║\n", fmtDur(direct.LatencyP50), fmtDur(proxy.LatencyP50))
	fmt.Printf("║  Latency p95      ║  %-13s ║  %-21s ║\n", fmtDur(direct.LatencyP95), fmtDur(proxy.LatencyP95))
	fmt.Printf("║  Latency p99      ║  %-13s ║  %-21s ║\n", fmtDur(direct.LatencyP99), fmtDur(proxy.LatencyP99))
	fmt.Printf("╠═══════════════════╩════════════════╩════════════════════════╣\n")
	fmt.Printf("║  Proxy Overhead (p50):  %-35s ║\n", fmt.Sprintf("%s (%.1f%%)", fmtDur(overhead), overheadPct))
	fmt.Printf("║  QPS Drop:              %-35s ║\n", fmt.Sprintf("%.1f%%", qpsDrop))
	fmt.Printf("╚═════════════════════════════════════════════════════════════╝\n")
}

func fmtDur(d time.Duration) string {
	us := float64(d.Microseconds())
	if us < 1000 {
		return fmt.Sprintf("%.0fµs", us)
	}
	return fmt.Sprintf("%.2fms", us/1000)
}