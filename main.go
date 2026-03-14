package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

func downloadJSON(ds Dataset, outDir string) error {
	filename := filepath.Join(outDir, ds.Name+".json")

	fmt.Printf("[%s] Starting download (paginated, %d rows/page)...\n", ds.Name, pageSize)

	client := &http.Client{
		Timeout: 0, // no overall timeout
		Transport: &http.Transport{
			ResponseHeaderTimeout: 60 * time.Second,
		},
	}

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
		url := fmt.Sprintf("%s/resource/%s.json?$limit=%d&$offset=%d&$order=:id", baseURL, ds.ID, pageSize, offset)

		resp, err := client.Get(url)
		if err != nil {
			return fmt.Errorf("request failed at offset %d: %w", offset, err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return fmt.Errorf("HTTP %d at offset %d", resp.StatusCode, offset)
		}

		var records []json.RawMessage
		err = json.NewDecoder(resp.Body).Decode(&records)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("JSON decode failed at offset %d: %w", offset, err)
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

func download(ds Dataset, outDir, format string) error {
	if format == "csv" {
		return downloadCSV(ds, outDir)
	}
	return downloadJSON(ds, outDir)
}

func downloadWithRetry(ds Dataset, outDir, format string) error {
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := download(ds, outDir, format)
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

func main() {
	outDir := flag.String("out", ".", "Output directory")
	format := flag.String("format", "json", "Output format: json or csv")
	concurrent := flag.Int("concurrent", 3, "Max concurrent downloads")
	list := flag.Bool("list", false, "List available datasets and exit")
	only := flag.String("only", "", "Comma-separated dataset names to download (default: all)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "rdw-download - Download RDW Gekentekende Voertuigen datasets\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n  rdw-download [flags]\n\nFlags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  rdw-download                                          # download all as JSON\n")
		fmt.Fprintf(os.Stderr, "  rdw-download -format csv -out ./data                   # download all as CSV to ./data\n")
		fmt.Fprintf(os.Stderr, "  rdw-download -only gekentekende_voertuigen_brandstof   # download one dataset\n")
		fmt.Fprintf(os.Stderr, "  rdw-download -list                                     # list available datasets\n")
	}

	flag.Parse()

	if *list {
		fmt.Println("Available datasets:")
		for _, ds := range allDatasets {
			fmt.Printf("  %-50s %s\n", ds.Name, ds.ID)
		}
		return
	}

	if *format != "json" && *format != "csv" {
		fmt.Fprintf(os.Stderr, "Error: format must be 'json' or 'csv'\n")
		os.Exit(1)
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

	// Create output directory
	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
		os.Exit(1)
	}

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
			if err := downloadWithRetry(ds, *outDir, *format); err != nil {
				fmt.Printf("[%s] %v. Skipping.\n", ds.Name, err)
				failed.Add(1)
			}
		}(ds)
	}

	wg.Wait()

	f := int(failed.Load())
	if f > 0 {
		fmt.Printf("\nDone with %d error(s).\n", f)
		os.Exit(1)
	}
	fmt.Println("\nAll done!")
}
