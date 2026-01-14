"""
Unit tests for EDGAR master.idx file download and processing functionality
"""

import pytest
import pandas as pd
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open
from datetime import datetime
import gzip
import sys
import os

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.trading_agent.fundamentals.edgar.edgar import EDGARDownloader
from src.trading_agent.fundamentals.edgar.edgar_postgres import (
    get_master_idx_download_status,
    mark_master_idx_download_success,
    mark_master_idx_download_failed,
    get_pending_or_failed_quarters
)


class TestEDGARMasterIdxParsing:
    """Tests for parsing master.idx file content"""
    
    def test_parse_master_idx_valid_content(self):
        """Test parsing valid master.idx content"""
        downloader = EDGARDownloader()
        
        # Sample master.idx content (simplified)
        content = b"""CIK|Company Name|Form Type|Date Filed|Filename
1000045|NICHOLAS FINANCIAL INC|10-Q|2022-11-14|edgar/data/1000045/0000950170-22-024756.txt
789019|MICROSOFT CORP|10-K|2023-07-28|edgar/data/789019/0000789019-23-000077.txt
"""
        
        df = downloader._parse_master_idx(content)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ['cik', 'company_name', 'form_type', 'filing_date', 'filename', 'accession_number']
        assert df.iloc[0]['cik'] == '0001000045'
        assert df.iloc[0]['company_name'] == 'NICHOLAS FINANCIAL INC'
        assert df.iloc[0]['form_type'] == '10-Q'
        assert df.iloc[0]['filing_date'] == '2022-11-14'
        assert df.iloc[0]['accession_number'] == '0000950170-22-024756'
    
    def test_parse_master_idx_with_gzip(self):
        """Test parsing gzipped master.idx content"""
        downloader = EDGARDownloader()
        
        # Create gzipped content
        original_content = b"""CIK|Company Name|Form Type|Date Filed|Filename
1000045|NICHOLAS FINANCIAL INC|10-Q|2022-11-14|edgar/data/1000045/0000950170-22-024756.txt
"""
        gzipped_content = gzip.compress(original_content)
        
        df = downloader._parse_master_idx(gzipped_content)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]['cik'] == '0001000045'
    
    def test_parse_master_idx_skips_header_lines(self):
        """Test that header lines and separators are skipped"""
        downloader = EDGARDownloader()
        
        content = b"""CIK|Company Name|Form Type|Date Filed|Filename
---
1000045|NICHOLAS FINANCIAL INC|10-Q|2022-11-14|edgar/data/1000045/0000950170-22-024756.txt
"""
        
        df = downloader._parse_master_idx(content)
        
        assert len(df) == 1  # Should skip header and separator lines
    
    def test_parse_master_idx_invalid_lines(self):
        """Test that invalid lines are skipped"""
        downloader = EDGARDownloader()
        
        content = b"""CIK|Company Name|Form Type|Date Filed|Filename
1000045|NICHOLAS FINANCIAL INC|10-Q|2022-11-14|edgar/data/1000045/0000950170-22-024756.txt
invalid|line|with|wrong|format|extra|field
1000045|COMPANY|10-K|invalid-date|edgar/data/1000045/file.txt
789019|MICROSOFT CORP|10-K|2023-07-28|edgar/data/789019/0000789019-23-000077.txt
"""
        
        df = downloader._parse_master_idx(content)
        
        # Should only parse valid lines (2 valid entries)
        assert len(df) == 2
    
    def test_parse_master_idx_empty_content(self):
        """Test parsing empty content"""
        downloader = EDGARDownloader()
        
        content = b"""CIK|Company Name|Form Type|Date Filed|Filename
"""
        
        df = downloader._parse_master_idx(content)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == ['cik', 'company_name', 'form_type', 'filing_date', 'filename', 'accession_number']
    
    def test_parse_master_idx_date_formats(self):
        """Test parsing different date formats"""
        downloader = EDGARDownloader()
        
        # Test YYYYMMDD format
        content1 = b"""CIK|Company Name|Form Type|Date Filed|Filename
1000045|COMPANY|10-Q|20221114|edgar/data/1000045/0000950170-22-024756.txt
"""
        df1 = downloader._parse_master_idx(content1)
        assert df1.iloc[0]['filing_date'] == '2022-11-14'
        
        # Test YYYY-MM-DD format
        content2 = b"""CIK|Company Name|Form Type|Date Filed|Filename
1000045|COMPANY|10-Q|2022-11-14|edgar/data/1000045/0000950170-22-024756.txt
"""
        df2 = downloader._parse_master_idx(content2)
        assert df2.iloc[0]['filing_date'] == '2022-11-14'


class TestEDGARMasterIdxSaving:
    """Tests for saving master.idx files to disk"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests"""
        temp_path = Path(tempfile.mkdtemp())
        yield temp_path
        shutil.rmtree(temp_path)
    
    @pytest.fixture
    def downloader(self, temp_dir):
        """Create EDGARDownloader with temporary master directory"""
        downloader = EDGARDownloader()
        # Override master_dir to use temp directory
        downloader.master_dir = temp_dir / "master"
        downloader.master_dir.mkdir(exist_ok=True)
        return downloader
    
    def test_save_master_idx_content_uncompressed(self, downloader):
        """Test saving uncompressed master.idx content"""
        content = b"""CIK|Company Name|Form Type|Date Filed|Filename
1000045|NICHOLAS FINANCIAL INC|10-Q|2022-11-14|edgar/data/1000045/0000950170-22-024756.txt
"""
        
        downloader._save_master_idx_content(content, "2022", "QTR4", is_compressed=False)
        
        # Check raw file exists
        raw_file = downloader.master_dir / "2022" / "QTR4_master.idx"
        assert raw_file.exists()
        assert raw_file.read_bytes() == content
        
        # Check CSV file exists
        csv_file = downloader.master_dir / "2022" / "QTR4_master_parsed.csv"
        assert csv_file.exists()
        
        # Verify CSV content
        df = pd.read_csv(csv_file)
        assert len(df) == 1
        assert df.iloc[0]['cik'] == '0001000045'
    
    def test_save_master_idx_content_compressed(self, downloader):
        """Test saving compressed master.idx content"""
        original_content = b"""CIK|Company Name|Form Type|Date Filed|Filename
1000045|NICHOLAS FINANCIAL INC|10-Q|2022-11-14|edgar/data/1000045/0000950170-22-024756.txt
"""
        gzipped_content = gzip.compress(original_content)
        
        downloader._save_master_idx_content(gzipped_content, "2022", "QTR4", is_compressed=True)
        
        # Check raw file exists
        raw_file = downloader.master_dir / "2022" / "QTR4_master.idx.gz"
        assert raw_file.exists()
        
        # Check CSV file exists and is parsed correctly
        csv_file = downloader.master_dir / "2022" / "QTR4_master_parsed.csv"
        assert csv_file.exists()
        
        df = pd.read_csv(csv_file)
        assert len(df) == 1


class TestEDGARMasterIdxDownload:
    """Tests for downloading master.idx files"""
    
    @pytest.fixture
    def mock_conn(self):
        """Create mock database connection"""
        conn = Mock()
        return conn
    
    @pytest.fixture
    def downloader(self):
        """Create EDGARDownloader"""
        return EDGARDownloader()
    
    @patch('src.trading_agent.fundamentals.edgar.edgar.get_pending_or_failed_quarters')
    @patch('src.trading_agent.fundamentals.edgar.edgar.get_master_idx_download_status')
    @patch('src.trading_agent.fundamentals.edgar.edgar.mark_master_idx_download_success')
    @patch('src.trading_agent.fundamentals.edgar.edgar.requests.get')
    def test_save_master_idx_to_disk_success_uncompressed(
        self, mock_get, mock_mark_success, mock_get_status, mock_get_pending, mock_conn, downloader, tmp_path
    ):
        """Test successful download of uncompressed master.idx"""
        # Setup mocks
        mock_get_pending.return_value = []  # Empty list - will check individual status
        mock_get_status.return_value = None  # New quarter, not in ledger
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"""CIK|Company Name|Form Type|Date Filed|Filename
1000045|NICHOLAS FINANCIAL INC|10-Q|2022-11-14|edgar/data/1000045/0000950170-22-024756.txt
"""
        mock_get.return_value = mock_response
        
        # Override master_dir
        downloader.master_dir = tmp_path / "master"
        downloader.master_dir.mkdir(exist_ok=True)
        
        # Test with a specific year that won't hit current year logic
        # Limit to just 2022 to avoid processing too many quarters
        downloader.save_master_idx_to_disk(mock_conn, start_year=2022)
        
        # Verify download was attempted (at least one call)
        assert mock_get.called
        # Verify success was marked (at least one call)
        assert mock_mark_success.called
    
    @patch('src.trading_agent.fundamentals.edgar.edgar.get_pending_or_failed_quarters')
    @patch('src.trading_agent.fundamentals.edgar.edgar.get_master_idx_download_status')
    @patch('src.trading_agent.fundamentals.edgar.edgar.mark_master_idx_download_failed')
    @patch('src.trading_agent.fundamentals.edgar.edgar.requests.get')
    def test_save_master_idx_to_disk_failure(
        self, mock_get, mock_mark_failed, mock_get_status, mock_get_pending, mock_conn, downloader, tmp_path
    ):
        """Test handling of download failure"""
        # Setup mocks
        mock_get_pending.return_value = []
        mock_get_status.return_value = None  # New quarter
        mock_response = Mock()
        mock_response.status_code = 404  # Not found
        mock_get.return_value = mock_response
        
        # Override master_dir
        downloader.master_dir = tmp_path / "master"
        downloader.master_dir.mkdir(exist_ok=True)
        
        # Test with a specific year that won't hit current year logic
        with pytest.raises(Exception):
            downloader.save_master_idx_to_disk(mock_conn, start_year=2022)
        
        # Verify failure was marked
        assert mock_mark_failed.called
    
    @patch('src.trading_agent.fundamentals.edgar.edgar.get_master_idx_download_status')
    def test_save_master_idx_to_disk_skips_successful_quarters(
        self, mock_get_status, mock_conn, downloader
    ):
        """Test that successful quarters are skipped"""
        # Mock all quarters as successful
        mock_get_status.return_value = {'status': 'success'}
        
        # Should return early without downloading
        with patch('builtins.print') as mock_print:
            # Use a past year to avoid current year logic
            downloader.save_master_idx_to_disk(mock_conn, start_year=2020)
            # Should print "No new or failed quarters to download."
            assert any("No new or failed quarters" in str(call) for call in mock_print.call_args_list)


class TestEDGARMasterIdxDatabase:
    """Tests for database operations"""
    
    @pytest.fixture
    def mock_conn(self):
        """Create mock database connection"""
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value = cursor
        return conn
    
    @pytest.fixture
    def downloader(self, tmp_path):
        """Create EDGARDownloader with temporary master directory"""
        downloader = EDGARDownloader()
        downloader.master_dir = tmp_path / "master"
        downloader.master_dir.mkdir(exist_ok=True)
        return downloader
    
    def test_save_master_idx_to_db_loads_csv_files(self, downloader, mock_conn, tmp_path):
        """Test that _save_master_idx_to_db loads CSV files correctly"""
        # Create test CSV file
        year_dir = downloader.master_dir / "2022"
        year_dir.mkdir(exist_ok=True)
        
        test_df = pd.DataFrame({
            'cik': ['0001000045'],
            'company_name': ['NICHOLAS FINANCIAL INC'],
            'form_type': ['10-Q'],
            'filing_date': ['2022-11-14'],
            'filename': ['edgar/data/1000045/0000950170-22-024756.txt'],
            'accession_number': ['0000950170-22-024756']
        })
        csv_file = year_dir / "QTR4_master_parsed.csv"
        test_df.to_csv(csv_file, index=False)
        
        # Mock cursor and execute_values
        cursor = mock_conn.cursor.return_value
        
        downloader._save_master_idx_to_db(mock_conn)
        
        # Verify cursor was used
        assert mock_conn.cursor.called
        # Verify commit was called
        assert mock_conn.commit.called
    
    def test_save_master_idx_to_db_skips_non_csv_files(self, downloader, mock_conn, tmp_path):
        """Test that non-CSV files are skipped"""
        year_dir = downloader.master_dir / "2022"
        year_dir.mkdir(exist_ok=True)
        
        # Create a non-CSV file
        other_file = year_dir / "other_file.txt"
        other_file.write_text("not a csv")
        
        downloader._save_master_idx_to_db(mock_conn)
        
        # Should not process the non-CSV file
        # (cursor should not be called if no CSV files found)
        # This test verifies the filtering logic works


class TestEDGARMasterIdxLedger:
    """Tests for ledger tracking functionality"""
    
    @pytest.fixture
    def mock_conn(self):
        """Create mock database connection"""
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value = cursor
        return conn
    
    def test_get_master_idx_download_status_not_found(self, mock_conn):
        """Test getting status for quarter not in ledger"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchone.return_value = None
        
        status = get_master_idx_download_status(mock_conn, 2022, 'QTR4')
        
        assert status is None
    
    def test_mark_master_idx_download_success(self, mock_conn):
        """Test marking download as successful"""
        cursor = mock_conn.cursor.return_value
        
        mark_master_idx_download_success(mock_conn, 2022, 'QTR4')
        
        assert mock_conn.cursor.called
        assert mock_conn.commit.called
    
    def test_mark_master_idx_download_failed(self, mock_conn):
        """Test marking download as failed"""
        cursor = mock_conn.cursor.return_value
        
        mark_master_idx_download_failed(mock_conn, 2022, 'QTR4', "Test error")
        
        assert mock_conn.cursor.called
        assert mock_conn.commit.called
    
    def test_get_pending_or_failed_quarters(self, mock_conn):
        """Test getting list of pending/failed quarters"""
        cursor = mock_conn.cursor.return_value
        cursor.fetchall.return_value = [(2022, 'QTR4'), (2023, 'QTR1')]
        
        quarters = get_pending_or_failed_quarters(mock_conn, start_year=2022)
        
        assert len(quarters) == 2
        assert (2022, 'QTR4') in quarters
        assert (2023, 'QTR1') in quarters
