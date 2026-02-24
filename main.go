package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"tenantsdb-bench/bench"
	"tenantsdb-bench/my"
	"tenantsdb-bench/pg"
)

func main() {
	cmd := flag.NewFlagSet("bench", flag.ExitOnError)

	dbType := cmd.String("db", "postgres", "Database type: postgres, mysql, mongodb, redis")
	testType := cmd.String("test", "overhead", "Test type: overhead, throughput, multi, isolation, scale")

	proxyHost := cmd.String("proxy-host", "", "Proxy host")
	proxyPort := cmd.Int("proxy-port", 0, "Proxy port")
	proxyUser := cmd.String("proxy-user", "", "Project ID")
	proxyPass := cmd.String("proxy-pass", "", "Proxy password")
	proxyDB := cmd.String("proxy-db", "", "Database name")

	directHost := cmd.String("direct-host", "", "Direct DB host")
	directPort := cmd.Int("direct-port", 0, "Direct DB port")
	directUser := cmd.String("direct-user", "", "Direct DB user")
	directPass := cmd.String("direct-pass", "", "Direct DB password")
	directDB := cmd.String("direct-db", "", "Direct DB name")

	queries := cmd.Int("queries", 10000, "Number of queries (count-based mode)")
	concurrency := cmd.Int("concurrency", 10, "Concurrent connections")
	warmup := cmd.Int("warmup", 100, "Warmup queries before measuring")
	seedRows := cmd.Int("seed-rows", 10000, "Rows to insert for test data")
	duration := cmd.Int("duration", 0, "Run duration in seconds (0 = use query count)")
	runs := cmd.Int("runs", 1, "Number of runs for median calculation (1 = single run)")

	cmd.Parse(os.Args[1:])

	if *proxyHost == "" {
		fmt.Println("Usage: tdb-bench [flags]")
		fmt.Println()
		fmt.Println("Required flags:")
		fmt.Println("  -proxy-host    Proxy host")
		fmt.Println("  -proxy-port    Proxy port")
		fmt.Println("  -proxy-user    Project ID")
		fmt.Println("  -proxy-pass    Proxy password")
		fmt.Println("  -proxy-db      Database name")
		fmt.Println()
		fmt.Println("For overhead test, also provide:")
		fmt.Println("  -direct-host   Direct DB host")
		fmt.Println("  -direct-port   Direct DB port")
		fmt.Println("  -direct-user   Direct DB user")
		fmt.Println("  -direct-pass   Direct DB password")
		fmt.Println("  -direct-db     Direct DB name")
		fmt.Println()
		fmt.Println("Options:")
		fmt.Println("  -db            Database type: postgres, mysql, mongodb, redis (default: postgres)")
		fmt.Println("  -test          Test type: overhead, throughput, multi, isolation, scale")
		fmt.Println("  -queries       Number of queries (default: 10000, ignored if -duration set)")
		fmt.Println("  -concurrency   Concurrent connections (default: 10)")
		fmt.Println("  -warmup        Warmup queries (default: 100)")
		fmt.Println("  -seed-rows     Test data rows (default: 10000)")
		fmt.Println("  -duration      Run duration in seconds (default: 0 = count-based)")
		fmt.Println("  -runs          Number of runs for median (default: 1)")
		os.Exit(1)
	}

	proxyCfg := bench.ConnConfig{
		Host:     *proxyHost,
		Port:     *proxyPort,
		User:     *proxyUser,
		Password: *proxyPass,
		Database: *proxyDB,
	}

	directCfg := bench.ConnConfig{
		Host:     *directHost,
		Port:     *directPort,
		User:     *directUser,
		Password: *directPass,
		Database: *directDB,
	}

	params := bench.BenchParams{
		Queries:     *queries,
		Concurrency: *concurrency,
		Warmup:      *warmup,
		SeedRows:    *seedRows,
		Duration:    time.Duration(*duration) * time.Second,
		Runs:        *runs,
	}

	if params.Duration > 0 {
		fmt.Printf("Mode: time-based (%ds per run", *duration)
	} else {
		fmt.Printf("Mode: count-based (%d queries per run", params.Queries)
	}
	if params.Runs > 1 {
		fmt.Printf(", %d runs, median reported)\n", params.Runs)
	} else {
		fmt.Println(", single run)")
	}

	switch *dbType {
	case "postgres":
		switch *testType {
		case "overhead":
			if *directHost == "" {
				fmt.Println("Error: overhead test requires -direct-* flags for comparison")
				os.Exit(1)
			}
			pg.RunOverhead(proxyCfg, directCfg, params)
		case "throughput":
			pg.RunThroughput(proxyCfg, params)
		case "multi":
			pg.RunMultiTenant(proxyCfg, params)
		case "isolation":
			pg.RunIsolation(proxyCfg, params)
		case "scale":
			pg.RunScale(proxyCfg, params)
		default:
			fmt.Printf("Unknown test type: %s\n", *testType)
			os.Exit(1)
		}
	case "mysql":
		switch *testType {
		case "overhead":
			if *directHost == "" {
				fmt.Println("Error: overhead test requires -direct-* flags for comparison")
				os.Exit(1)
			}
			my.RunOverhead(proxyCfg, directCfg, params)
		case "throughput":
			my.RunThroughput(proxyCfg, params)
		case "multi":
			my.RunMultiTenant(proxyCfg, params)
		case "isolation":
			my.RunIsolation(proxyCfg, params)
		case "scale":
			my.RunScale(proxyCfg, params)
		default:
			fmt.Printf("Unknown test type: %s\n", *testType)
			os.Exit(1)
		}
	default:
		fmt.Printf("Database type '%s' not yet implemented\n", *dbType)
		os.Exit(1)
	}
}