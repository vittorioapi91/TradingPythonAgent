# TradingPythonAgent

A Python trading agent project for algorithmic trading and market analysis.

## Project Structure

```
TradingPythonAgent/
├── src/
│   └── trading_agent/
│       ├── __init__.py
│       └── agent.py
├── tests/
│   └── __init__.py
├── requirements.txt
├── .gitignore
└── main.py
```

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the main script:
```bash
python main.py
```

## Development

Add your trading logic and strategies in the `src/trading_agent/` directory.

## Downloading FRED Economic Data

This project includes functionality to download time series data from FRED (Federal Reserve Economic Data).

### Setup

1. Get a free FRED API key from: https://fred.stlouisfed.org/docs/api/api_key.html

2. Set your API key as an environment variable:
```bash
export FRED_API_KEY="your_api_key_here"
```

Or pass it directly when using the downloader.

### Usage

#### Command Line

Download all available FRED economic variables:
```bash
python src/trading_agent/macro/main.py --api-key YOUR_API_KEY
```

With options:
```bash
python src/trading_agent/macro/main.py \
    --api-key YOUR_API_KEY \
    --start-date 2000-01-01 \
    --end-date 2024-01-01 \
    --limit 1000 \
    --output-dir fred_data \
    --format both
```

#### Python Script

```python
from src.trading_agent.macro import FREDDataDownloader

# Initialize downloader (uses FRED_API_KEY env var if available)
downloader = FREDDataDownloader(api_key="your_api_key")

# Download all available series
data = downloader.download_all_available_series(
    start_date="2000-01-01",
    end_date="2024-01-01",
    limit=1000,  # Optional: limit number of series
    output_dir="fred_data",
    save_format="both"  # 'csv', 'parquet', or 'both'
)

# Or download specific series
series_data = downloader.download_series("GDP", start_date="2020-01-01")

# Download multiple specific series
df = downloader.download_multiple_series(
    ["GDP", "UNRATE", "CPIAUCSL"],
    start_date="2020-01-01"
)
```

### Features

- Discovers all available FRED economic variables
- Downloads time series data with date range filtering
- Handles API rate limiting automatically
- Saves data in CSV and/or Parquet formats
- Includes series metadata (title, units, frequency, etc.)
- Efficient batch downloading with error handling

### Output

The downloader creates:
- `fred_all_series.csv` / `fred_all_series.parquet`: Combined DataFrame with all series
- `series_metadata.csv`: Metadata for each series (title, units, frequency, etc.)

All files are saved in the specified output directory (default: `fred_data/`).
