"""Macro economic model module for downloading economic data"""

try:
    from .fred.fred_data_downloader import FREDDataDownloader
except ImportError:
    try:
        from .fred_data_downloader import FREDDataDownloader
    except ImportError:
        FREDDataDownloader = None

from .imf.imf_data_downloader import IMFDataDownloader
try:
    from .bis.bis_data_downloader import BISDataDownloader
except ImportError:
    try:
        from .bis_data_downloader import BISDataDownloader
    except ImportError:
        BISDataDownloader = None

__all__ = ['IMFDataDownloader']
if FREDDataDownloader:
    __all__.append('FREDDataDownloader')
if BISDataDownloader:
    __all__.append('BISDataDownloader')
