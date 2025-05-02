import pandas as pd
from astroquery.sdss import SDSS
from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy.table import Table
from .utils import log_message, safe_to_csv, safe_to_fits, handle_exception, validate_coordinates, get_sdss_config

class PhotometryFetcher:
    """
    PhotometryFetcher
    -----------------
    Fetches SDSS ugriz photometric magnitudes for given coordinates or objid.

    Parameters:
    - data_release (int): SDSS data release to use (default: 16)
    - cache_results (bool): Whether to cache query results (default: True)

    This class is designed to work in a pipeline. It supports fetching photometry
    directly from a DataFrame (e.g. `targets` with 'ra' and 'dec' columns) just like CutoutFetcher.

    Methods:
    - fetch_by_coord(ra, dec, radius): Search PhotoObj near (RA, DEC)
    - fetch_all(df): Fetch photometry for all rows with 'ra' and 'dec'
    - fetch_by_objid(objid): Query ugriz magnitudes by SDSS objid
    - save_to_csv(df, filename): Save a photometry DataFrame to CSV
    - save_to_fits(df, filename): Save a photometry DataFrame to FITS
    """

    def __init__(self, data_release: int = 16, cache_results: bool = True):
        self.data_release = data_release
        self.cache_results = cache_results
        self.config = get_sdss_config(data_release)
        self._cache = {}  # Simple in-memory cache

    def fetch_by_coord(self, ra: float, dec: float, radius: float = 5.0) -> pd.DataFrame:
        """
        Fetch photometry for objects near the given coordinates.
        
        Parameters:
        - ra: Right Ascension in degrees
        - dec: Declination in degrees
        - radius: Search radius in arcseconds
        
        Returns:
        - DataFrame with photometry data
        """
        if not validate_coordinates(ra, dec):
            return pd.DataFrame()
            
        # Check cache first
        cache_key = f"{ra:.6f}_{dec:.6f}_{radius}"
        if self.cache_results and cache_key in self._cache:
            log_message(f"Using cached photometry for RA={ra}, DEC={dec}")
            return self._cache[cache_key]
            
        try:
            coord = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, frame='icrs')
            result = SDSS.query_region(
                coord,
                radius=radius * u.arcsec,
                photoobj_fields=["ra", "dec", "objid", "u", "g", "r", "i", "z"],
                data_release=self.data_release
            )
            if result is None or len(result) == 0:
                log_message(f"No photometric object found near RA={ra}, DEC={dec}")
                return pd.DataFrame()
                
            df = result.to_pandas()
            df["objid"] = df["objid"].astype(str)
            log_message(f"Found {len(df)} photo objects near RA={ra}, DEC={dec}")
            
            # Cache the result
            if self.cache_results:
                self._cache[cache_key] = df.copy()
                
            return df
        except Exception as e:
            handle_exception("fetch_by_coord", e)
            return pd.DataFrame()

    def fetch_all(self, df: pd.DataFrame, ra_col: str = "ra", dec_col: str = "dec", radius: float = 5.0) -> pd.DataFrame:
        """
        Fetch photometry for all coordinates in a DataFrame.
        
        Parameters:
        - df: DataFrame with RA/DEC columns
        - ra_col: Name of RA column
        - dec_col: Name of DEC column
        - radius: Search radius in arcseconds
        
        Returns:
        - DataFrame with photometry data for all objects
        """
        all_rows = []
        for idx, row in df.iterrows():
            ra = row[ra_col]
            dec = row[dec_col]
            log_message(f"Processing row {idx}: RA={ra}, DEC={dec}")
            result = self.fetch_by_coord(ra=ra, dec=dec, radius=radius)
            if not result.empty:
                # Add reference to original row index
                result['source_idx'] = idx
                all_rows.append(result)
                
        if all_rows:
            result_df = pd.concat(all_rows, ignore_index=True)
            log_message(f"Fetched photometry for {len(result_df)} objects from {len(df)} coordinates")
            return result_df
        else:
            log_message("No photometry found for any coordinates")
            return pd.DataFrame()

    def fetch_by_objid(self, objid: int) -> dict:
        """
        Fetch ugriz photometry for a specific SDSS PhotoObjID.
        
        Parameters:
        - objid: SDSS PhotoObj ID
        
        Returns:
        - Dictionary with photometry data
        """
        # Check cache first
        cache_key = f"objid_{objid}"
        if self.cache_results and cache_key in self._cache:
            log_message(f"Using cached photometry for objid={objid}")
            return self._cache[cache_key]
            
        try:
            query = f"SELECT ra, dec, u, g, r, i, z FROM PhotoObjAll WHERE objid = {objid}"
            result = SDSS.query_sql(query, data_release=self.data_release)
            if result is None or len(result) == 0:
                log_message(f"No photometry found for objid={objid}")
                return {}
                
            row = result[0]
            photo_data = {
                "ra": row["ra"],
                "dec": row["dec"],
                "u": row["u"],
                "g": row["g"],
                "r": row["r"],
                "i": row["i"],
                "z": row["z"]
            }
            
            # Cache the result
            if self.cache_results:
                self._cache[cache_key] = photo_data.copy()
                
            log_message(f"Fetched photometry for objid={objid}")
            return photo_data
        except Exception as e:
            handle_exception("fetch_by_objid", e)
            return {}

    def save_to_csv(self, df: pd.DataFrame, filename: str):
        """
        Save photometry DataFrame to CSV.
        
        Parameters:
        - df: DataFrame to save
        - filename: Output filename
        """
        try:
            safe_to_csv(df, filename)
        except Exception as e:
            handle_exception("save_to_csv", e)

    def save_to_fits(self, df: pd.DataFrame, filename: str):
        """
        Save photometry DataFrame to FITS.
        
        Parameters:
        - df: DataFrame to save
        - filename: Output filename
        """
        try:
            safe_to_fits(df, filename)
        except Exception as e:
            handle_exception("save_to_fits", e)
            
    def clear_cache(self):
        """
        Clear the internal cache.
        """
        cache_size = len(self._cache)
        self._cache = {}
        log_message(f"Cleared photometry cache ({cache_size} entries)")
