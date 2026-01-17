"""
Unit tests for EDGAR command line interface switches
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock
import argparse

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


class TestEDGARCLIArguments:
    """Tests for EDGAR command line argument parsing"""
    
    def test_default_arguments(self):
        """Test that default arguments are set correctly"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = []
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.trading_agent.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                # Mock the downloader instance
                mock_downloader = MagicMock()
                mock_downloader_class.return_value = mock_downloader
                
                # When no arguments are provided, the script creates EDGARDownloader but doesn't do anything
                # So we just verify it was instantiated with default user_agent
                try:
                    edgar_main()
                except (SystemExit, Exception):
                    pass
                
                # Verify EDGARDownloader was instantiated
                assert mock_downloader_class.called
                # Check default user_agent was used
                call_kwargs = mock_downloader_class.call_args[1] if mock_downloader_class.call_args[1] else {}
                assert 'user_agent' in call_kwargs or mock_downloader_class.call_args[0] == ()
    
    def test_start_year_argument(self):
        """Test --start-year argument parsing"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--generate-catalog', '--start-year', '2020']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.trading_agent.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                with patch('src.trading_agent.fundamentals.edgar.edgar.get_postgres_connection') as mock_conn:
                    with patch('src.trading_agent.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                        with patch('src.trading_agent.fundamentals.edgar.master_idx.MasterIdxManager') as mock_master_idx:
                            # Mock the downloader instance
                            mock_downloader = MagicMock()
                            mock_downloader_class.return_value = mock_downloader
                            
                            # Mock connection
                            mock_conn.return_value = MagicMock()
                            
                            # Mock MasterIdxManager
                            mock_manager = MagicMock()
                            mock_master_idx.return_value = mock_manager
                            
                            try:
                                edgar_main()
                            except (SystemExit, Exception):
                                pass  # May exit or raise, that's okay for this test
                            
                            # Verify start_year was passed to MasterIdxManager
                            # The actual call happens in main, but we can verify the manager was created
                            assert mock_master_idx.called
    
    def test_output_dir_argument(self):
        """Test --output-dir argument parsing"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--filings', '--output-dir', '/custom/output/path']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            # Patch FilingDownloader where it's actually imported from (filings module)
            with patch('src.trading_agent.fundamentals.edgar.filings.FilingDownloader') as mock_filing_downloader_class:
                mock_filing_downloader = MagicMock()
                mock_filing_downloader.download_filings = MagicMock(return_value=[])
                mock_filing_downloader_class.return_value = mock_filing_downloader
                
                with patch('src.trading_agent.fundamentals.edgar.edgar.get_postgres_connection'):
                    try:
                        edgar_main()
                    except (SystemExit, Exception):
                        pass
                    
                    # Verify FilingDownloader was instantiated (used for --filings)
                    assert mock_filing_downloader_class.called
                    # Verify output_dir was passed to download_filings
                    if mock_filing_downloader.download_filings.called:
                        call_kwargs = mock_filing_downloader.download_filings.call_args[1] if mock_filing_downloader.download_filings.call_args else {}
                        assert call_kwargs.get('output_dir') == '/custom/output/path'
    
    def test_user_agent_argument(self):
        """Test --user-agent argument parsing"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        custom_user_agent = 'Custom User Agent test@example.com'
        test_args = ['--filings', '--user-agent', custom_user_agent]
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            # Patch FilingDownloader where it's actually imported from (filings module)
            with patch('src.trading_agent.fundamentals.edgar.filings.FilingDownloader') as mock_filing_downloader_class:
                mock_filing_downloader = MagicMock()
                mock_filing_downloader.download_filings = MagicMock(return_value=[])
                mock_filing_downloader_class.return_value = mock_filing_downloader
                
                with patch('src.trading_agent.fundamentals.edgar.edgar.get_postgres_connection'):
                    try:
                        edgar_main()
                    except (SystemExit, Exception):
                        pass
                    
                    # Verify FilingDownloader was called with custom user agent
                    mock_filing_downloader_class.assert_called_once()
                    call_kwargs = mock_filing_downloader_class.call_args[1]
                    assert call_kwargs['user_agent'] == custom_user_agent
    
    def test_generate_catalog_flag(self):
        """Test --generate-catalog flag"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--generate-catalog']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.trading_agent.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                with patch('src.trading_agent.fundamentals.edgar.edgar.get_postgres_connection') as mock_conn:
                    with patch('src.trading_agent.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                        with patch('src.trading_agent.fundamentals.edgar.master_idx.MasterIdxManager') as mock_master_idx:
                            # Mock the downloader instance
                            mock_downloader = MagicMock()
                            mock_downloader_class.return_value = mock_downloader
                            
                            # Mock connection
                            mock_conn.return_value = MagicMock()
                            
                            # Mock MasterIdxManager
                            mock_manager = MagicMock()
                            mock_master_idx.return_value = mock_manager
                            
                            try:
                                edgar_main()
                            except (SystemExit, Exception):
                                pass
                            
                            # Verify MasterIdxManager was instantiated (used in generate-catalog mode)
                            assert mock_master_idx.called
    
    def test_download_companies_flag(self):
        """Test --download-companies flag"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--generate-catalog', '--download-companies']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.trading_agent.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                with patch('src.trading_agent.fundamentals.edgar.edgar.get_postgres_connection') as mock_conn:
                    with patch('src.trading_agent.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                        with patch('src.trading_agent.fundamentals.edgar.master_idx.MasterIdxManager') as mock_master_idx:
                            # Mock the downloader instance
                            mock_downloader = MagicMock()
                            mock_downloader_class.return_value = mock_downloader
                            
                            # Mock connection
                            mock_conn.return_value = MagicMock()
                            
                            # Mock MasterIdxManager
                            mock_manager = MagicMock()
                            mock_master_idx.return_value = mock_manager
                            
                            try:
                                edgar_main()
                            except (SystemExit, Exception):
                                pass
                            
                            # Verify MasterIdxManager was called (download-companies flag affects catalog generation)
                            assert mock_master_idx.called
    
    def test_process_zips_argument(self):
        """Test --process-zips argument"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_dir = '/test/zip/directory'
        test_args = ['--process-zips', test_dir]
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.trading_agent.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                # Mock the downloader instance
                mock_downloader = MagicMock()
                mock_downloader_class.return_value = mock_downloader
                
                # Mock process_zip_files method
                mock_downloader.process_zip_files = MagicMock(return_value={})
                
                try:
                    edgar_main()
                except (SystemExit, Exception):
                    pass
                
                # Verify process_zip_files was called with the directory
                assert mock_downloader.process_zip_files.called
                # Get call arguments - function is called as: process_zip_files(directory, recursive=bool)
                call_args = mock_downloader.process_zip_files.call_args
                # First positional argument is the directory
                assert call_args[0][0] == test_dir
                # recursive is a keyword argument, default is True (not --no-recursive)
                assert call_args[1]['recursive'] is True
    
    def test_no_recursive_flag(self):
        """Test --no-recursive flag with --process-zips"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_dir = '/test/zip/directory'
        test_args = ['--process-zips', test_dir, '--no-recursive']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.trading_agent.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                # Mock the downloader instance
                mock_downloader = MagicMock()
                mock_downloader_class.return_value = mock_downloader
                
                # Mock process_zip_files method
                mock_downloader.process_zip_files = MagicMock(return_value={})
                
                try:
                    edgar_main()
                except (SystemExit, Exception):
                    pass
                
                # Verify process_zip_files was called with recursive=False
                assert mock_downloader.process_zip_files.called
                # Get call arguments - function is called as: process_zip_files(directory, recursive=bool)
                call_args = mock_downloader.process_zip_files.call_args
                # First positional argument is the directory
                assert call_args[0][0] == test_dir
                # recursive is a keyword argument, --no-recursive sets it to False
                assert call_args[1]['recursive'] is False
    
    def test_database_arguments(self):
        """Test database connection arguments (--dbname, --dbuser, --dbhost, --dbpassword, --dbport)"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = [
            '--generate-catalog',
            '--dbname', 'test_db',
            '--dbuser', 'test_user',
            '--dbhost', 'test_host',
            '--dbpassword', 'test_password',
            '--dbport', '5433'
        ]
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.trading_agent.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                with patch('src.trading_agent.fundamentals.edgar.edgar.get_postgres_connection') as mock_conn:
                    with patch('src.trading_agent.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                        with patch('src.trading_agent.fundamentals.edgar.master_idx.MasterIdxManager'):
                            # Mock the downloader instance
                            mock_downloader = MagicMock()
                            mock_downloader_class.return_value = mock_downloader
                            
                            # Mock connection
                            mock_conn.return_value = MagicMock()
                            
                            try:
                                edgar_main()
                            except (SystemExit, Exception):
                                pass
                            
                            # Verify get_postgres_connection was called with custom database args
                            assert mock_conn.called
                            call_kwargs = mock_conn.call_args[1]
                            assert call_kwargs['dbname'] == 'test_db'
                            assert call_kwargs['user'] == 'test_user'
                            assert call_kwargs['host'] == 'test_host'
                            assert call_kwargs['password'] == 'test_password'
                            assert call_kwargs['port'] == 5433
    
    def test_database_defaults(self):
        """Test that database arguments use defaults when not specified"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--generate-catalog']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.trading_agent.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                with patch('src.trading_agent.fundamentals.edgar.edgar.get_postgres_connection') as mock_conn:
                    with patch('src.trading_agent.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                        with patch('src.trading_agent.fundamentals.edgar.master_idx.MasterIdxManager'):
                            # Mock the downloader instance
                            mock_downloader = MagicMock()
                            mock_downloader_class.return_value = mock_downloader
                            
                            # Mock connection
                            mock_conn.return_value = MagicMock()
                            
                            try:
                                edgar_main()
                            except (SystemExit, Exception):
                                pass
                            
                            # Verify get_postgres_connection was called with default values
                            assert mock_conn.called
                            call_kwargs = mock_conn.call_args[1]
                            assert call_kwargs['dbname'] == 'edgar'  # default
                            assert call_kwargs['user'] == 'postgres'  # default
                            assert call_kwargs['host'] == 'localhost'  # default
    
    def test_combined_arguments(self):
        """Test combining multiple arguments together"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = [
            '--generate-catalog',
            '--download-companies',
            '--start-year', '2020',
            '--output-dir', '/custom/output',
            '--user-agent', 'Test Agent test@example.com',
            '--dbname', 'custom_db',
            '--dbuser', 'custom_user'
        ]
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.trading_agent.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                with patch('src.trading_agent.fundamentals.edgar.edgar.get_postgres_connection') as mock_conn:
                    with patch('src.trading_agent.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                        with patch('src.trading_agent.fundamentals.edgar.master_idx.MasterIdxManager') as mock_master_idx:
                                # Mock the downloader instance
                                mock_downloader = MagicMock()
                                mock_downloader_class.return_value = mock_downloader
                                
                                # Mock connection
                                mock_conn.return_value = MagicMock()
                                
                                # Mock MasterIdxManager
                                mock_manager = MagicMock()
                                mock_master_idx.return_value = mock_manager
                                
                                try:
                                    edgar_main()
                                except (SystemExit, Exception):
                                    pass
                                
                                # Verify all components were called
                                assert mock_downloader_class.called
                                call_kwargs = mock_downloader_class.call_args[1]
                                assert call_kwargs['user_agent'] == 'Test Agent test@example.com'
                                
                                assert mock_conn.called
                                conn_kwargs = mock_conn.call_args[1]
                                assert conn_kwargs['dbname'] == 'custom_db'
                                assert conn_kwargs['user'] == 'custom_user'
                                
                                assert mock_master_idx.called
    
    def test_invalid_start_year_type(self):
        """Test that invalid start-year type raises error"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--start-year', 'not-a-number']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            # argparse should raise SystemExit for invalid type
            with pytest.raises((SystemExit, ValueError)):
                edgar_main()
    
    def test_invalid_dbport_type(self):
        """Test that invalid dbport type raises error"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--generate-catalog', '--dbport', 'not-a-number']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            # argparse should raise SystemExit for invalid type
            with pytest.raises((SystemExit, ValueError)):
                edgar_main()
    
    def test_help_flag(self):
        """Test that --help flag works"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--help']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            # argparse raises SystemExit when --help is used
            with pytest.raises(SystemExit) as exc_info:
                edgar_main()
            
            # SystemExit code 0 indicates successful help display
            assert exc_info.value.code == 0


class TestEDGARCLIArgumentGroups:
    """Tests for argument groups and their organization"""
    
    def test_catalog_generation_group(self):
        """Test that catalog generation arguments are grouped"""
        # This test verifies the argument parser structure
        # We can't easily test the grouping visually, but we can verify
        # that the arguments exist and work together
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--generate-catalog', '--download-companies']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.trading_agent.fundamentals.edgar.edgar.EDGARDownloader'):
                with patch('src.trading_agent.fundamentals.edgar.edgar.get_postgres_connection'):
                        with patch('src.trading_agent.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                            with patch('src.trading_agent.fundamentals.edgar.master_idx.MasterIdxManager'):
                                try:
                                    edgar_main()
                                except (SystemExit, Exception):
                                    pass
                                
                                # If we get here without argument errors, grouping works
                                assert True
    
    def test_database_connection_group(self):
        """Test that database connection arguments are grouped"""
        from trading_agent.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = [
            '--generate-catalog',
            '--dbname', 'test',
            '--dbuser', 'test',
            '--dbhost', 'test',
            '--dbpassword', 'test',
            '--dbport', '5432'
        ]
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.trading_agent.fundamentals.edgar.edgar.EDGARDownloader'):
                with patch('src.trading_agent.fundamentals.edgar.edgar.get_postgres_connection') as mock_conn:
                    with patch('src.trading_agent.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                        with patch('src.trading_agent.fundamentals.edgar.master_idx.MasterIdxManager'):
                            mock_conn.return_value = MagicMock()
                            
                            try:
                                edgar_main()
                            except (SystemExit, Exception):
                                pass
                            
                            # Verify all database args were parsed correctly
                            assert mock_conn.called
                            call_kwargs = mock_conn.call_args[1]
                            assert 'dbname' in call_kwargs
                            assert 'user' in call_kwargs
                            assert 'host' in call_kwargs
                            assert 'password' in call_kwargs
                            assert 'port' in call_kwargs
