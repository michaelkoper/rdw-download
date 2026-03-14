# rdw-download

CLI tool to download and query [RDW Gekentekende Voertuigen](https://opendata.rdw.nl) (Dutch registered vehicles) datasets.

Downloads all 8 datasets in JSON, CSV, or directly into a SQLite database:

| Dataset | ID |
|---------|-----|
| gekentekende_voertuigen | m9d7-ebf2 |
| gekentekende_voertuigen_brandstof | 8ys7-d773 |
| gekentekende_voertuigen_carrosserie | vezc-m2t6 |
| gekentekende_voertuigen_carrosserie_specificatie | jhie-znh9 |
| gekentekende_voertuigen_voertuigklasse | kmfi-hrps |
| gekentekende_voertuigen_assen | 3huj-srit |
| gekentekende_voertuigen_rupsbanden | 3xwf-ince |
| gekentekende_voertuigen_subcategorie_voertuig | 2ba7-embk |

## Installation

### Download a prebuilt binary

Download the binary for your platform from the releases and place it somewhere in your PATH.

#### macOS

```bash
mv rdw-download /usr/local/bin/
chmod +x /usr/local/bin/rdw-download
```

#### Linux

```bash
mv rdw-download-linux /usr/local/bin/rdw-download
chmod +x /usr/local/bin/rdw-download
```

#### Windows

1. Download `rdw-download.exe`
2. Move it to a folder of your choice, e.g. `C:\Tools\`
3. Add that folder to your PATH:
   - Open **Start** and search for **"Environment Variables"**
   - Click **"Edit the system environment variables"**
   - Click **Environment Variables...**
   - Under **User variables**, select **Path** and click **Edit**
   - Click **New** and add `C:\Tools`
   - Click **OK** to save

Or just run it directly:
```
.\rdw-download.exe
```

### Build from source

Requires [Go 1.21+](https://go.dev/dl/).

```bash
git clone https://github.com/michaelkoper/rdw-download.git
cd rdw-download
go build -o rdw-download .
```

Cross-compile for other platforms:

```bash
GOOS=windows GOARCH=amd64 go build -o rdw-download.exe .
GOOS=linux   GOARCH=amd64 go build -o rdw-download-linux .
```

## Usage

```
rdw-download [flags]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-format` | `json` | Output format: `json`, `csv`, or `sqlite` |
| `-out` | `.` | Output directory (for json/csv) |
| `-db` | | SQLite database path (implies `-format sqlite`) |
| `-concurrent` | `3` | Max concurrent downloads (json/csv only) |
| `-only` | *(all)* | Comma-separated dataset names to download |
| `-list` | | List available datasets and exit |
| `-query` | | Run a SQL query on the database and exit |
| `-query-format` | `table` | Query output format: `table`, `csv`, or `json` |

### Download examples

```bash
# Download all datasets as JSON
rdw-download

# Download all as CSV into a ./data folder
rdw-download -format csv -out ./data

# Download only the fuel dataset
rdw-download -only gekentekende_voertuigen_brandstof

# Download two specific datasets as CSV
rdw-download -only gekentekende_voertuigen_brandstof,gekentekende_voertuigen_assen -format csv

# List available datasets
rdw-download -list
```

### SQLite examples

```bash
# Download all datasets into a SQLite database
rdw-download -db rdw.db

# Download one dataset into SQLite
rdw-download -db rdw.db -only gekentekende_voertuigen_brandstof

# List tables
rdw-download -db rdw.db -query "SELECT name FROM sqlite_master WHERE type='table'"

# Top 10 car brands
rdw-download -db rdw.db -query "SELECT merk, COUNT(*) as cnt FROM gekentekende_voertuigen GROUP BY merk ORDER BY cnt DESC LIMIT 10"

# Find all Teslas (output as JSON)
rdw-download -db rdw.db -query "SELECT * FROM gekentekende_voertuigen WHERE merk='TESLA' LIMIT 5" -query-format json

# Export query results as CSV
rdw-download -db rdw.db -query "SELECT kenteken, merk, handelsbenaming FROM gekentekende_voertuigen WHERE merk='TOYOTA'" -query-format csv > toyotas.csv
```

### Notes

- **JSON** downloads use the SODA API with pagination (50k rows/page), producing a clean JSON array of objects. JSON/CSV downloads run concurrently.
- **CSV** downloads use the bulk export endpoint — a single streaming request per dataset, much faster.
- **SQLite** downloads use the SODA API with pagination and insert rows in batches. Each dataset becomes a table. Downloads run sequentially since SQLite only supports one writer at a time.
- The main `gekentekende_voertuigen` dataset contains ~16.7 million rows. Make sure you have enough disk space and a stable internet connection.
- No CGO or C compiler required — the SQLite engine is pure Go, so all binaries work out of the box on macOS, Linux, and Windows.
