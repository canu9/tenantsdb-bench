package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	cmd := flag.NewFlagSet("bench", flag.ExitOnError)

	// Test selection
	dbType := cmd.String("db", "postgres", "Database type: postgres, mysql, mongodb, redis")
	testType := cmd.String("test", "overhead", "Test type: overhead, throughput, isolation")

	// Proxy connection (through TenantsDB)
	proxyHost := cmd.String("proxy-host", "", "Proxy host (e.g., 192.168.60.13)")
	proxyPort := cmd.Int("proxy-port", 0, "Proxy port (e.g., 30432)")
	proxyUser := cmd.String("proxy-user", "", "Project ID (e.g., tdb_24cbcee9)")
	proxyPass := cmd.String("proxy-pass", "", "Proxy password (e.g., tdb_7288175b98bafbae)")
	proxyDB := cmd.String("proxy-db", "", "Database name (e.g., bench_pg__bench01)")

	// Direct connection (bypass proxy)
	directHost := cmd.String("direct-host", "", "Direct DB host (e.g., 192.168.60.12)")
	directPort := cmd.Int("direct-port", 0, "Direct DB port (e.g., 5432)")
	directUser := cmd.String("direct-user", "", "Direct DB user")
	directPass := cmd.String("direct-pass", "", "Direct DB password")
	directDB := cmd.String("direct-db", "", "Direct DB name")

	// Benchmark parameters
	queries := cmd.Int("queries", 10000, "Number of queries to run")
	concurrency := cmd.Int("concurrency", 10, "Concurrent connections")
	warmup := cmd.Int("warmup", 100, "Warmup queries before measuring")
	seedRows := cmd.Int("seed-rows", 10000, "Rows to insert for test data")

	cmd.Parse(os.Args[1:])

	if *proxyHost == "" {
		fmt.Println("Usage: tenantsdb-bench [flags]")
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
		fmt.Println("  -test          Test type: overhead, throughput, isolation (default: overhead)")
		fmt.Println("  -queries       Number of queries (default: 10000)")
		fmt.Println("  -concurrency   Concurrent connections (default: 10)")
		fmt.Println("  -warmup        Warmup queries (default: 100)")
		fmt.Println("  -seed-rows     Test data rows (default: 10000)")
		os.Exit(1)
	}

	proxyCfg := ConnConfig{
		Host:     *proxyHost,
		Port:     *proxyPort,
		User:     *proxyUser,
		Password: *proxyPass,
		Database: *proxyDB,
	}

	directCfg := ConnConfig{
		Host:     *directHost,
		Port:     *directPort,
		User:     *directUser,
		Password: *directPass,
		Database: *directDB,
	}

	params := BenchParams{
		Queries:     *queries,
		Concurrency: *concurrency,
		Warmup:      *warmup,
		SeedRows:    *seedRows,
	}

	switch *dbType {
	case "postgres":
		switch *testType {
		case "overhead":
			if *directHost == "" {
				fmt.Println("Error: overhead test requires -direct-* flags for comparison")
				os.Exit(1)
			}
			RunPostgresOverhead(proxyCfg, directCfg, params)
		case "throughput":
			RunPostgresThroughput(proxyCfg, params)
		default:
			fmt.Printf("Unknown test type: %s\n", *testType)
			os.Exit(1)
		}
	default:
		fmt.Printf("Database type '%s' not yet implemented\n", *dbType)
		os.Exit(1)
	}
}