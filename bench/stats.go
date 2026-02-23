package bench

import (
	"math"
	"sort"
	"time"
)

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
	stats.LatencyP50 = pct(durations, 50)
	stats.LatencyP75 = pct(durations, 75)
	stats.LatencyP90 = pct(durations, 90)
	stats.LatencyP95 = pct(durations, 95)
	stats.LatencyP99 = pct(durations, 99)
	stats.QPS = float64(len(durations)) / totalDuration.Seconds()

	return stats
}

// MedianStats picks the median run by p50 latency from multiple runs.
func MedianStats(runs []BenchStats) BenchStats {
	if len(runs) == 1 {
		return runs[0]
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].LatencyP50 < runs[j].LatencyP50 })
	return runs[len(runs)/2]
}

// SteadyState checks if QPS variance across runs is within tolerance.
func SteadyState(runs []BenchStats, tolerance float64) (bool, float64) {
	if len(runs) < 2 {
		return true, 0
	}
	var sum float64
	for _, r := range runs {
		sum += r.QPS
	}
	mean := sum / float64(len(runs))
	if mean == 0 {
		return false, 0
	}

	var maxDev float64
	for _, r := range runs {
		dev := math.Abs(r.QPS-mean) / mean
		if dev > maxDev {
			maxDev = dev
		}
	}
	return maxDev <= tolerance, maxDev
}

func pct(sorted []time.Duration, p float64) time.Duration {
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