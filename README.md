# rdw-download

CLI tool to download [RDW Gekentekende Voertuigen](https://opendata.rdw.nl) (Dutch registered vehicles) datasets.

Downloads all 8 datasets concurrently in JSON or CSV format:

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
# Move to a directory in your PATH
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
| `-format` | `json` | Output format: `json` or `csv` |
| `-out` | `.` | Output directory |
| `-concurrent` | `3` | Max concurrent downloads |
| `-only` | *(all)* | Comma-separated dataset names to download |
| `-list` | | List available datasets and exit |

### Examples

```bash
# Download all datasets as JSON to the current directory
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

### Notes

- **JSON** downloads use the SODA API with pagination (50k rows/page), producing a clean JSON array of objects. This is slower but gives you well-structured data.
- **CSV** downloads use the bulk export endpoint — a single streaming request per dataset, much faster.
- The main `gekentekende_voertuigen` dataset contains ~16.7 million rows. In JSON format this is ~30 GB, in CSV ~5 GB. Make sure you have enough disk space and a stable internet connection.
