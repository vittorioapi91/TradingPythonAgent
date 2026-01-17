# Release Notes

## v0.0.2 - Airflow DAGs for EDGAR Filings and Improved CLI

**Release Date:** January 17, 2025

### 🎉 New Features

#### Airflow DAGs for EDGAR Filings
- **`edgar_filings_download` DAG**: Automated daily downloads of SEC EDGAR filings from database
  - Configurable filters: year, quarter, form_type, company_name, cik, limit
  - Environment-aware (dev/staging/prod) with appropriate default limits
  - Supports manual triggering with custom parameters via DAG run config
  - Default form type: 10-K filings

- **`edgar_filings_quarterly` DAG**: Automated quarterly filing downloads
  - Runs monthly (1st of each month at 2 AM)
  - Automatically downloads filings from the previous quarter
  - Limited to 1000 filings in non-prod environments

#### Enhanced EDGAR CLI
- **New `--filings` flag**: Replaces `--from-db` flag
  - Filings always come from database (master_idx table)
  - No need for explicit `--from-db` flag
  - Supports comprehensive filter arguments:
    - `--year`: Filter by year (e.g., 2005)
    - `--quarter`: Filter by quarter (QTR1, QTR2, QTR3, QTR4)
    - `--form-type`: Filter by form type (e.g., 10-K, 10-Q)
    - `--company-name`: Filter by company name (partial match, case-insensitive)
    - `--cik`: Filter by CIK (Central Index Key)
    - `--limit`: Limit number of filings to download

#### Improved Airflow Integration
- **Enhanced wheel detection**: Airflow plugin now properly detects installed wheels
  - Checks if package is importable (most reliable method)
  - Falls back to checking installed distributions
  - Shows wheel file availability if package not yet installed
- **Wheel verification script**: Added `verify-wheel-installation.sh` for troubleshooting

### 🔧 Improvements

- **Launch configurations**: All launch.json files are now tracked in version control
- **Test updates**: Fixed tests to use new `--filings` flag instead of deprecated `--from-db`
- **Documentation**: Added comprehensive README for Airflow DAGs with usage examples

### 🗑️ Removed

- **`--from-db` flag**: Removed from `edgar.py` CLI (filings always come from database)
- **`test_from_db_flag` test**: Removed obsolete test

### 📝 Technical Details

#### Files Added
- `.ops/.airflow/dags/edgar_filings_dag.py` - Main filings download DAG
- `.ops/.airflow/dags/edgar_filings_quarterly_dag.py` - Quarterly automated downloads
- `.ops/.airflow/dags/README.md` - Comprehensive DAG documentation
- `.ops/.airflow/verify-wheel-installation.sh` - Wheel verification utility
- `src/fundamentals/edgar/filings.py` - Filing download functionality
- `src/fundamentals/edgar/filings_postgres.py` - PostgreSQL query builders
- `tests/trading_agent/fundamentals/edgar/test_filings.py` - Comprehensive test suite

#### Files Modified
- `src/fundamentals/edgar/edgar.py` - Added `--filings` flag and filter arguments
- `.ops/.airflow/plugins/environment_info/plugin.py` - Improved wheel detection
- `.vscode/launch.json` - Updated to use `--filings` flag
- `.vscode/fundamentals/launch.json` - Updated configurations
- `tests/trading_agent/fundamentals/edgar/test_edgar_cli.py` - Updated tests
- `.gitignore` - Ensure all launch.json files are tracked

### 🚀 Usage Examples

#### Download filings via CLI
```bash
# Download 10-K filings for Q2 2005
python -m trading_agent.fundamentals.edgar.edgar \
    --filings \
    --year 2005 \
    --quarter QTR2 \
    --form-type 10-K \
    --output-dir ./filings

# Download filings for a specific company
python -m trading_agent.fundamentals.edgar.edgar \
    --filings \
    --company-name "NVIDIA" \
    --form-type 10-K \
    --year 2023
```

#### Trigger Airflow DAG with custom parameters
```json
{
  "year": 2005,
  "quarter": "QTR2",
  "form_type": "10-K",
  "limit": 50
}
```

### 📊 Statistics

- **20 files changed**: 1,958 insertions(+), 209 deletions(-)
- **New DAGs**: 2
- **New test files**: 1
- **New utility scripts**: 1

### 🔗 Related Issues

- DEV-5: Add flexible filing download functionality
- DEV-5: Add Airflow DAGs for EDGAR filings download

---

## v0.0.1 - Initial Release

**Release Date:** (Previous release)

Initial release of TradingPythonAgent with core functionality for:
- SEC EDGAR filings download
- Macroeconomic data collection (FRED, BLS, BIS, Eurostat, IMF)
- Market data collection (Yahoo Finance, iShares ETFs)
- HMM model training and serving
- MLflow integration
- Kubernetes deployment
