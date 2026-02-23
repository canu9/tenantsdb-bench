package bench

import "time"

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
	Duration    time.Duration // 0 = use Queries count, >0 = time-based
	Runs        int           // number of runs for median (0 = single run)
}

type QueryResult struct {
	At       time.Time
	Duration time.Duration
	Err      error
}

type BenchStats struct {
	Label      string
	Total      int
	Errors     int
	Duration   time.Duration
	QPS        float64
	LatencyAvg time.Duration
	LatencyMin time.Duration
	LatencyMax time.Duration
	LatencyP50 time.Duration
	LatencyP75 time.Duration
	LatencyP90 time.Duration
	LatencyP95 time.Duration
	LatencyP99 time.Duration
}