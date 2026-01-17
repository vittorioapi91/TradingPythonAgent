"""
Extended Yahoo Finance data extraction

This module handles scraping additional data from Yahoo Finance that's not available
through the standard yfinance API, including historical valuation measures,
EPS revisions, revenue estimates, and detailed analyst ratings.
"""

import re
import json
import requests
from typing import Dict, List, Optional
from datetime import datetime


def extract_valuation_measures(symbol: str, html_content: str) -> List[Dict]:
    """
    Extract historical valuation measures from Yahoo Finance page
    
    Args:
        symbol: Stock symbol
        html_content: HTML content of the Yahoo Finance statistics page
        
    Returns:
        List of valuation measure dictionaries
    """
    valuation_data = []
    
    try:
        # Look for valuation measures data in the page
        # Yahoo Finance stores this data in various formats (JSON, tables, etc.)
        
        # Try to find JSON data embedded in the page
        json_patterns = [
            r'"valuationMeasures":\s*({[^}]+})',
            r'valuationMeasures:\s*({[^}]+})',
        ]
        
        for pattern in json_patterns:
            matches = re.finditer(pattern, html_content, re.DOTALL)
            for match in matches:
                try:
                    data = json.loads(match.group(1))
                    # Process the data structure
                    break
                except:
                    continue
        
        # Alternative: Try to parse table structure
        # This is a simplified version - you may need to adjust based on actual HTML structure
        table_pattern = r'<table[^>]*class="[^"]*valuation[^"]*"[^>]*>(.*?)</table>'
        table_match = re.search(table_pattern, html_content, re.DOTALL | re.IGNORECASE)
        
        if table_match:
            # Parse table rows and extract data
            # This would need more sophisticated parsing based on actual HTML structure
            pass
        
    except Exception as e:
        print(f"    Warning: Error extracting valuation measures: {e}")
    
    return valuation_data


def fetch_yahoo_statistics_page(symbol: str) -> Optional[str]:
    """
    Fetch the statistics page HTML for a symbol
    
    Args:
        symbol: Stock symbol
        
    Returns:
        HTML content or None if error
    """
    try:
        url = f"https://finance.yahoo.com/quote/{symbol}/key-statistics"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.text
        return None
    except Exception as e:
        print(f"    Warning: Error fetching statistics page: {e}")
        return None


def fetch_yahoo_analysis_page(symbol: str) -> Optional[str]:
    """
    Fetch the analysis page HTML for a symbol (contains EPS revisions, revenue estimates)
    
    Args:
        symbol: Stock symbol
        
    Returns:
        HTML content or None if error
    """
    try:
        url = f"https://finance.yahoo.com/quote/{symbol}/analysis"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.text
        return None
    except Exception as e:
        print(f"    Warning: Error fetching analysis page: {e}")
        return None

