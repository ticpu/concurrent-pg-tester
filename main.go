package main

import (
	"database/sql"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

type Result struct {
	TotalTime, ConnectTime, QueryTime, FetchTime time.Duration
	Failed                                       bool
}

func min(times []time.Duration) time.Duration {
	min := times[0]
	for _, duration := range times[1:] {
		if duration < min {
			min = duration
		}
	}
	return min
}

func max(times []time.Duration) time.Duration {
	max := times[0]
	for _, duration := range times[1:] {
		if duration > max {
			max = duration
		}
	}
	return max
}

func avg(times []time.Duration) time.Duration {
	sum := time.Duration(0)
	for _, duration := range times {
		sum += duration
	}
	return time.Duration(int64(sum) / int64(len(times)))
}

func median(times []time.Duration) time.Duration {
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	if len(times)%2 == 0 {
		return (times[len(times)/2-1] + times[len(times)/2]) / 2
	}
	return times[len(times)/2]
}

func worker(id int, start *sync.WaitGroup, done chan<- Result, dbName, username, password, host, query string, earlyConnect bool) {
	var (
		conn        *sql.DB
		connectTime time.Duration
		err         error
		result      = Result{Failed: true}
	)
	defer func(id int, result *Result) {
		logrus.Debugf("Thread %d: Returning result", id)
		done <- *result
	}(id, &result) // Always send a result when the goroutine exits

	if earlyConnect {
		conn, connectTime, err = connect(id, dbName, username, password, host)
		if err != nil {
			logrus.Errorf("Thread %d: %v", id, err)
			return
		}
		defer conn.Close()
		start.Wait()
	}

	logrus.Debugf("Thread %d: ready", id)
	start.Wait()
	startTime := time.Now()

	if !earlyConnect {
		conn, connectTime, err = connect(id, dbName, username, password, host)
		if err != nil {
			logrus.Errorf("Thread %d: %v", id, err)
			return
		}
		defer conn.Close()
	}

	query = strings.Replace(query, "####", strconv.Itoa(id), -1)
	logrus.Debugf("Thread %d: Executing query", id)
	rows, err := conn.Query(query)
	if err != nil {
		logrus.Errorf("Thread %d: %v", id, err)
		return
	}

	queryTime := time.Since(startTime)
	startTime = time.Now()

	logrus.Debugf("Thread %d: Fetching SQL data", id)
	rows.Next()
	_ = rows.Close()
	fetchTime := time.Since(startTime)
	result.TotalTime = connectTime + queryTime + fetchTime
	result.ConnectTime = connectTime
	result.QueryTime = queryTime
	result.FetchTime = fetchTime
	result.Failed = false
}

func connect(id int, dbName, username, password, host string) (*sql.DB, time.Duration, error) {
	start := time.Now()
	logrus.Debugf("Thread %d: Connecting to the database", id)

	connStr := fmt.Sprintf("dbname=%s user=%s host=%s sslmode=disable", dbName, username, host)
	if password != "" {
		connStr = fmt.Sprintf("%s password=%s", connStr, password)
	}

	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, 0, err
	}

	return conn, time.Since(start), nil
}

func main() {
	var (
		err          error
		dbName       string
		username     string
		password     string
		host         string
		workers      int
		query        string
		earlyConnect bool
		verbose      int
	)

	pflag.StringVarP(&dbName, "db-name", "d", "", "Database name (required)")
	pflag.StringVarP(&username, "username", "u", "", "Username (required)")
	pflag.StringVarP(&password, "password", "p", "", "Password")
	pflag.StringVarP(&host, "host", "h", "localhost", "Host")
	pflag.IntVarP(&workers, "workers", "w", 500, "Number of worker threads")
	pflag.StringVarP(&query, "query", "q", "", "SQL query to test (required)")
	pflag.BoolVarP(&earlyConnect, "early-connect", "e", false, "Connect during thread preparation")
	pflag.CountVarP(&verbose, "verbose", "v", "Increase verbosity level")

	pflag.Parse()

	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{
		Cur: uint64(workers) * 4,
		Max: uint64(workers) * 4,
	})

	if err != nil {
		logrus.Fatalf("Error setting rlimit: %v", err)
	}

	if dbName == "" || username == "" || query == "" {
		logrus.Fatal("Missing required flags")
	}

	if verbose >= 2 {
		logrus.SetLevel(logrus.DebugLevel)
	} else if verbose == 1 {
		logrus.SetLevel(logrus.InfoLevel)
	}

	start := &sync.WaitGroup{}
	start.Add(1)
	done := make(chan Result, workers)

	logrus.Info("Preparing workers")

	for i := 0; i < workers; i++ {
		go worker(i, start, done, dbName, username, password, host, query, earlyConnect)
	}

	logrus.Info("Starting queries")
	start.Done()

	totalTimes := make([]time.Duration, 0, workers)
	connectTimes := make([]time.Duration, 0, workers)
	queryTimes := make([]time.Duration, 0, workers)
	fetchTimes := make([]time.Duration, 0, workers)
	failed := 0

	logrus.Info("Fetching results")
	for i := 0; i < workers; i++ {
		result := <-done

		if result.Failed {
			failed++
			continue
		}

		totalTimes = append(totalTimes, result.TotalTime)
		connectTimes = append(connectTimes, result.ConnectTime)
		queryTimes = append(queryTimes, result.QueryTime)
		fetchTimes = append(fetchTimes, result.FetchTime)
	}

	logrus.Info("All queries completed")
	fmt.Printf("Failed queries: %d\n", failed)

	if len(totalTimes) > 0 {
		fmt.Printf("Total times: min=%v, median=%v, avg=%v, max=%v\n", min(totalTimes), median(totalTimes), avg(totalTimes), max(totalTimes))
		fmt.Printf("Connect times: min=%v, median=%v, avg=%v, max=%v\n", min(connectTimes), median(connectTimes), avg(connectTimes), max(connectTimes))
		fmt.Printf("Query times: min=%v, median=%v, avg=%v, max=%v\n", min(queryTimes), median(queryTimes), avg(queryTimes), max(queryTimes))
		fmt.Printf("Fetch times: min=%v, median=%v, avg=%v, max=%v\n", min(fetchTimes), median(fetchTimes), avg(fetchTimes), max(fetchTimes))
	} else {
		fmt.Printf("No successful queries to calculate statistics.\n")
	}
}
