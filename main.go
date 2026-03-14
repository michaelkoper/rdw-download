package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

type Dataset struct {
	Name string
	ID   string
}

var allDatasets = []Dataset{
	{"gekentekende_voertuigen", "m9d7-ebf2"},
	{"gekentekende_voertuigen_brandstof", "8ys7-d773"},
	{"gekentekende_voertuigen_carrosserie", "vezc-m2t6"},
	{"gekentekende_voertuigen_carrosserie_specificatie", "jhie-znh9"},
	{"gekentekende_voertuigen_voertuigklasse", "kmfi-hrps"},
	{"gekentekende_voertuigen_assen", "3huj-srit"},
	{"gekentekende_voertuigen_rupsbanden", "3xwf-ince"},
	{"gekentekende_voertuigen_subcategorie_voertuig", "2ba7-embk"},
}

const baseURL = "https://opendata.rdw.nl"
const maxRetries = 3
const pageSize = 50000

// --- HTTP client ---

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			ResponseHeaderTimeout: 60 * time.Second,
		},
	}
}

// --- Progress writer for streaming downloads ---

type progressWriter struct {
	file    *os.File
	written atomic.Int64
	name    string
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.file.Write(p)
	pw.written.Add(int64(n))
	return n, err
}

// --- CSV download ---

func downloadCSV(ds Dataset, outDir string) error {
	url := fmt.Sprintf("%s/api/views/%s/rows.csv?accessType=DOWNLOAD", baseURL, ds.ID)
	filename := filepath.Join(outDir, ds.Name+".csv")

	fmt.Printf("[%s] Starting download...\n", ds.Name)

	client := &http.Client{Timeout: 30 * time.Minute}

	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer file.Close()

	pw := &progressWriter{file: file, name: ds.Name}

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				mb := float64(pw.written.Load()) / 1_048_576
				fmt.Printf("[%s] %.1f MB downloaded...\n", ds.Name, mb)
			case <-done:
				return
			}
		}
	}()

	_, err = io.Copy(pw, resp.Body)
	close(done)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	mb := float64(pw.written.Load()) / 1_048_576
	fmt.Printf("[%s] Done! %s (%.1f MB)\n", ds.Name, filename, mb)
	return nil
}

// --- JSON download ---

func downloadJSON(ds Dataset, outDir string) error {
	filename := filepath.Join(outDir, ds.Name+".json")

	fmt.Printf("[%s] Starting download (paginated, %d rows/page)...\n", ds.Name, pageSize)

	client := newHTTPClient()

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer file.Close()

	enc := json.NewEncoder(file)

	file.WriteString("[\n")

	offset := 0
	totalRows := 0

	for {
		records, err := fetchPage(client, ds.ID, offset)
		if err != nil {
			return err
		}

		if len(records) == 0 {
			break
		}

		for _, rec := range records {
			if totalRows > 0 {
				file.WriteString(",\n")
			}
			enc.Encode(json.RawMessage(rec))
			totalRows++
		}

		offset += pageSize
		info, _ := file.Stat()
		mb := float64(info.Size()) / 1_048_576
		fmt.Printf("[%s] %.1f MB written (%d rows so far)...\n", ds.Name, mb, totalRows)

		if len(records) < pageSize {
			break
		}
	}

	file.WriteString("]\n")

	info, _ := file.Stat()
	mb := float64(info.Size()) / 1_048_576
	fmt.Printf("[%s] Done! %s (%.1f MB, %d rows)\n", ds.Name, filename, mb, totalRows)
	return nil
}

// --- SQLite download ---

func downloadSQLite(ds Dataset, dbPath string) error {
	fmt.Printf("[%s] Starting download into SQLite (paginated, %d rows/page)...\n", ds.Name, pageSize)

	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL")
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	client := newHTTPClient()

	offset := 0
	totalRows := 0
	tableCreated := false
	var columns []string

	for {
		records, err := fetchPage(client, ds.ID, offset)
		if err != nil {
			return err
		}

		if len(records) == 0 {
			break
		}

		// Parse records into maps
		var rows []map[string]interface{}
		for _, rec := range records {
			var row map[string]interface{}
			if err := json.Unmarshal(rec, &row); err != nil {
				return fmt.Errorf("parse record: %w", err)
			}
			rows = append(rows, row)
		}

		// Create table from first batch
		if !tableCreated {
			columns = discoverColumns(rows)
			if err := createTable(db, ds.Name, columns); err != nil {
				return err
			}
			tableCreated = true
		}

		// Insert in transaction
		if err := insertRows(db, ds.Name, columns, rows); err != nil {
			return err
		}

		totalRows += len(records)
		offset += pageSize
		fmt.Printf("[%s] %d rows inserted...\n", ds.Name, totalRows)

		if len(records) < pageSize {
			break
		}
	}

	fmt.Printf("[%s] Done! %d rows inserted into table %q\n", ds.Name, totalRows, ds.Name)
	return nil
}

func discoverColumns(rows []map[string]interface{}) []string {
	seen := map[string]bool{}
	var columns []string
	for _, row := range rows {
		for k := range row {
			if !seen[k] {
				seen[k] = true
				columns = append(columns, k)
			}
		}
	}
	sort.Strings(columns)
	return columns
}

func createTable(db *sql.DB, tableName string, columns []string) error {
	var cols []string
	for _, c := range columns {
		cols = append(cols, fmt.Sprintf("%q TEXT", c))
	}
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %q (%s)", tableName, strings.Join(cols, ", "))
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("create table %q: %w", tableName, err)
	}
	return nil
}

func insertRows(db *sql.DB, tableName string, columns []string, rows []map[string]interface{}) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = fmt.Sprintf("%q", c)
	}
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf("INSERT INTO %q (%s) VALUES (%s)",
		tableName,
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "))

	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		vals := make([]interface{}, len(columns))
		for i, c := range columns {
			if v, ok := row[c]; ok {
				vals[i] = fmt.Sprintf("%v", v)
			} else {
				vals[i] = nil
			}
		}
		if _, err := stmt.Exec(vals...); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert row: %w", err)
		}
	}

	return tx.Commit()
}

// --- Fetch a page from SODA API ---

func fetchPage(client *http.Client, datasetID string, offset int) ([]json.RawMessage, error) {
	url := fmt.Sprintf("%s/resource/%s.json?$limit=%d&$offset=%d&$order=:id", baseURL, datasetID, pageSize, offset)

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("request failed at offset %d: %w", offset, err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP %d at offset %d", resp.StatusCode, offset)
	}

	var records []json.RawMessage
	err = json.NewDecoder(resp.Body).Decode(&records)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("JSON decode failed at offset %d: %w", offset, err)
	}

	return records, nil
}

// --- Query ---

func runQuery(dbPath, query, outputFormat string) error {
	db, err := sql.Open("sqlite", dbPath+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns: %w", err)
	}

	switch outputFormat {
	case "csv":
		return queryOutputCSV(rows, columns)
	case "json":
		return queryOutputJSON(rows, columns)
	default:
		return queryOutputTable(rows, columns)
	}
}

func scanRow(rows *sql.Rows, columns []string) ([]string, error) {
	values := make([]sql.NullString, len(columns))
	ptrs := make([]interface{}, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	result := make([]string, len(columns))
	for i, v := range values {
		if v.Valid {
			result[i] = v.String
		}
	}
	return result, nil
}

func queryOutputTable(rows *sql.Rows, columns []string) error {
	var allRows [][]string
	for rows.Next() {
		row, err := scanRow(rows, columns)
		if err != nil {
			return err
		}
		allRows = append(allRows, row)
	}

	// Calculate column widths
	widths := make([]int, len(columns))
	for i, c := range columns {
		widths[i] = len(c)
	}
	for _, row := range allRows {
		for i, v := range row {
			if len(v) > widths[i] {
				widths[i] = len(v)
			}
			if widths[i] > 40 {
				widths[i] = 40
			}
		}
	}

	// Print header
	for i, c := range columns {
		if i > 0 {
			fmt.Print(" | ")
		}
		fmt.Printf("%-*s", widths[i], c)
	}
	fmt.Println()
	for i := range columns {
		if i > 0 {
			fmt.Print("-+-")
		}
		fmt.Print(strings.Repeat("-", widths[i]))
	}
	fmt.Println()

	// Print rows
	for _, row := range allRows {
		for i, v := range row {
			if i > 0 {
				fmt.Print(" | ")
			}
			if len(v) > 40 {
				v = v[:37] + "..."
			}
			fmt.Printf("%-*s", widths[i], v)
		}
		fmt.Println()
	}

	fmt.Printf("\n(%d rows)\n", len(allRows))
	return nil
}

func queryOutputCSV(rows *sql.Rows, columns []string) error {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	w.Write(columns)
	for rows.Next() {
		row, err := scanRow(rows, columns)
		if err != nil {
			return err
		}
		w.Write(row)
	}
	return nil
}

func queryOutputJSON(rows *sql.Rows, columns []string) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	var results []map[string]interface{}
	for rows.Next() {
		row, err := scanRow(rows, columns)
		if err != nil {
			return err
		}
		record := make(map[string]interface{})
		for i, c := range columns {
			if row[i] != "" {
				record[c] = row[i]
			}
		}
		results = append(results, record)
	}
	return enc.Encode(results)
}

// --- Download dispatcher ---

func download(ds Dataset, outDir, format, dbPath string) error {
	switch format {
	case "csv":
		return downloadCSV(ds, outDir)
	case "sqlite":
		return downloadSQLite(ds, dbPath)
	default:
		return downloadJSON(ds, outDir)
	}
}

func downloadWithRetry(ds Dataset, outDir, format, dbPath string) error {
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := download(ds, outDir, format, dbPath)
		if err == nil {
			return nil
		}
		fmt.Printf("[%s] Error: %v (attempt %d/%d)\n", ds.Name, err, attempt, maxRetries)
		if attempt < maxRetries {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
		}
	}
	return fmt.Errorf("failed after %d attempts", maxRetries)
}

// --- Main ---

func main() {
	outDir := flag.String("out", ".", "Output directory (for json/csv) or database file (for sqlite)")
	format := flag.String("format", "json", "Output format: json, csv, or sqlite")
	concurrent := flag.Int("concurrent", 3, "Max concurrent downloads")
	list := flag.Bool("list", false, "List available datasets and exit")
	only := flag.String("only", "", "Comma-separated dataset names to download (default: all)")
	dbPath := flag.String("db", "", "SQLite database path (shorthand for -format sqlite -out <path>)")
	query := flag.String("query", "", "Run a SQL query on the database and exit")
	queryFormat := flag.String("query-format", "table", "Query output format: table, csv, or json")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "rdw-download - Download and query RDW Gekentekende Voertuigen datasets\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n  rdw-download [flags]\n\nFlags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  rdw-download                                          # download all as JSON\n")
		fmt.Fprintf(os.Stderr, "  rdw-download -format csv -out ./data                   # download all as CSV to ./data\n")
		fmt.Fprintf(os.Stderr, "  rdw-download -only gekentekende_voertuigen_brandstof   # download one dataset\n")
		fmt.Fprintf(os.Stderr, "  rdw-download -list                                     # list available datasets\n")
		fmt.Fprintf(os.Stderr, "\nSQLite:\n")
		fmt.Fprintf(os.Stderr, "  rdw-download -db rdw.db                                # download all into SQLite\n")
		fmt.Fprintf(os.Stderr, "  rdw-download -db rdw.db -only gekentekende_voertuigen  # download one dataset into SQLite\n")
		fmt.Fprintf(os.Stderr, "  rdw-download -db rdw.db -query \"SELECT merk, COUNT(*) as cnt FROM gekentekende_voertuigen GROUP BY merk ORDER BY cnt DESC LIMIT 10\"\n")
		fmt.Fprintf(os.Stderr, "  rdw-download -db rdw.db -query \"SELECT * FROM gekentekende_voertuigen WHERE merk='TESLA' LIMIT 5\" -query-format json\n")
		fmt.Fprintf(os.Stderr, "  rdw-download -db rdw.db -query \"SELECT name FROM sqlite_master WHERE type='table'\"\n")
	}

	flag.Parse()

	// -db implies -format sqlite
	if *dbPath != "" && *format == "json" {
		*format = "sqlite"
	}

	// Query mode
	if *query != "" {
		if *dbPath == "" {
			fmt.Fprintf(os.Stderr, "Error: -db is required when using -query\n")
			os.Exit(1)
		}
		if err := runQuery(*dbPath, *query, *queryFormat); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if *list {
		fmt.Println("Available datasets:")
		for _, ds := range allDatasets {
			fmt.Printf("  %-50s %s\n", ds.Name, ds.ID)
		}
		return
	}

	if *format != "json" && *format != "csv" && *format != "sqlite" {
		fmt.Fprintf(os.Stderr, "Error: format must be 'json', 'csv', or 'sqlite'\n")
		os.Exit(1)
	}

	if *format == "sqlite" && *dbPath == "" {
		*dbPath = filepath.Join(*outDir, "rdw.db")
	}

	// Determine which datasets to download
	datasets := allDatasets
	if *only != "" {
		selected := map[string]bool{}
		for _, name := range strings.Split(*only, ",") {
			selected[strings.TrimSpace(name)] = true
		}
		datasets = nil
		for _, ds := range allDatasets {
			if selected[ds.Name] {
				datasets = append(datasets, ds)
				delete(selected, ds.Name)
			}
		}
		for name := range selected {
			fmt.Fprintf(os.Stderr, "Warning: unknown dataset %q\n", name)
		}
		if len(datasets) == 0 {
			fmt.Fprintf(os.Stderr, "Error: no valid datasets selected. Use -list to see available names.\n")
			os.Exit(1)
		}
	}

	// Create output directory (for json/csv, or parent dir for sqlite)
	if *format == "sqlite" {
		if dir := filepath.Dir(*dbPath); dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				fmt.Fprintf(os.Stderr, "Error creating directory: %v\n", err)
				os.Exit(1)
			}
		}
	} else {
		if err := os.MkdirAll(*outDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
			os.Exit(1)
		}
	}

	if *format == "sqlite" {
		// Sequential for SQLite (single writer)
		fmt.Printf("Downloading %d dataset(s) into %s...\n\n", len(datasets), *dbPath)
		failCount := 0
		for _, ds := range datasets {
			if err := downloadWithRetry(ds, *outDir, *format, *dbPath); err != nil {
				fmt.Printf("[%s] %v. Skipping.\n", ds.Name, err)
				failCount++
			}
		}
		if failCount > 0 {
			fmt.Printf("\nDone with %d error(s).\n", failCount)
			os.Exit(1)
		}
	} else {
		// Concurrent for JSON/CSV
		fmt.Printf("Downloading %d dataset(s) as %s to %s (max %d concurrent)...\n\n", len(datasets), *format, *outDir, *concurrent)

		sem := make(chan struct{}, *concurrent)
		var wg sync.WaitGroup
		var failed atomic.Int32

		for _, ds := range datasets {
			wg.Add(1)
			go func(ds Dataset) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				if err := downloadWithRetry(ds, *outDir, *format, *dbPath); err != nil {
					fmt.Printf("[%s] %v. Skipping.\n", ds.Name, err)
					failed.Add(1)
				}
			}(ds)
		}

		wg.Wait()

		if f := int(failed.Load()); f > 0 {
			fmt.Printf("\nDone with %d error(s).\n", f)
			os.Exit(1)
		}
	}

	fmt.Println("\nAll done!")
}
