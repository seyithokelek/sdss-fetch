from astroquery.sdss import SDSS
from astropy.coordinates import SkyCoord
import astropy.units as u
import pandas as pd
from typing import Union, Dict, Optional
from .utils import log_message, handle_exception, validate_coordinates, get_sdss_config

class TargetResolver:
    """
    TargetResolver
    ---------------
    Resolves SDSS spectroscopic objects near given RA/DEC coordinates using Astroquery.
    
    Parameters:
    - radius_arcsec (float): Search radius in arcseconds (default: 10.0)
    - data_release (int): SDSS data release to use (default: 16)
    - cache_results (bool): Whether to cache query results (default: True)
    
    Methods:
    - resolve_target(ra, dec, select): Find spectroscopic objects near coordinates
    - resolve_all(df, ra_col, dec_col, select): Resolve all coordinates in DataFrame
    - resolve_by_pmfs(plate, mjd, fiberid, select): Find object by plate-MJD-fiber
    - clear_cache(): Clear the internal cache
    """

    def __init__(self, radius_arcsec: float = 10.0, data_release: int = 16, cache_results: bool = True):
        self.radius = radius_arcsec * u.arcsec
        self.data_release = data_release
        self.config = get_sdss_config(data_release)
        self.cache_results = cache_results
        self._cache = {}

    def resolve_target(self, ra: float, dec: float, select: int = 0) -> Optional[Dict]:
        if not validate_coordinates(ra, dec):
            return None
            
        cache_key = f"{ra:.6f}_{dec:.6f}"
        if self.cache_results and cache_key in self._cache:
            log_message(f"Using cached target for RA={ra}, DEC={dec}", print_console=False)
            return self._cache[cache_key]
            
        try:
            coord = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, frame='fk5')
            result = SDSS.query_region(coord, spectro=True, radius=self.radius, data_release=self.data_release)

            if result is None or len(result) == 0:
                log_message(f"No SDSS match found near RA={ra}, DEC={dec}", print_console=False)
                return None

            df = result.to_pandas()
            df["specobjid"] = df["specobjid"].astype(str)
            df = df[["plate", "mjd", "fiberID", "specobjid", "ra", "dec"]]

            log_message(f"RA={ra}, DEC={dec} → {len(df)} match(es) found", print_console=False)

            print(f"\n[Match list for RA={ra:.6f}, DEC={dec:.6f}]")
            print(df.to_string(index=True))

            if not isinstance(select, int) or select < 0 or select >= len(df):
                log_message(f"Invalid select index: {select} → defaulting to 0", print_console=False)
                select = 0

            chosen_row = df.iloc[[select]]
            chosen_dict = chosen_row.to_dict(orient="records")[0]

            log_message(
                f"Selected: plate={chosen_dict['plate']}, mjd={chosen_dict['mjd']}, "
                f"fiberID={chosen_dict['fiberID']}, specobjid={chosen_dict['specobjid']}",
                print_console=False
            )

            print(f"\n→ Selected row: {select}")
            print(chosen_row.squeeze().to_string())
            
            if self.cache_results:
                self._cache[cache_key] = chosen_dict.copy()

            return chosen_dict
        except Exception as e:
            handle_exception("resolve_target", e, print_console=False)
            return None

    def resolve_all(self, df: pd.DataFrame, ra_col: str = "ra", dec_col: str = "dec", select: int = 0) -> pd.DataFrame:
        results = []
        for i, row in df.iterrows():
            ra = row[ra_col]
            dec = row[dec_col]
            log_message(f"Resolving row {i}: RA={ra}, DEC={dec}...", print_console=False)
            result = self.resolve_target(ra, dec, select=select)
            if result:
                result['source_idx'] = i
                results.append(result)
            else:
                log_message(f"No match found at row {i}", print_console=False)
                
        if results:
            result_df = pd.DataFrame(results).reset_index(drop=True)
            log_message(f"Resolved {len(result_df)} targets from {len(df)} coordinates")
            return result_df
        else:
            log_message("No targets resolved from any coordinates")
            return pd.DataFrame()

    def resolve_by_pmfs(self, plate: int, mjd: int, fiberid: int, select: int = 0) -> Optional[Dict]:
        cache_key = f"pmf_{plate}_{mjd}_{fiberid}"
        if self.cache_results and cache_key in self._cache:
            log_message(f"Using cached target for plate={plate}, mjd={mjd}, fiberID={fiberid}", print_console=False)
            return self._cache[cache_key]
            
        try:
            result = SDSS.query_specobj(plate=plate, mjd=mjd, fiberID=fiberid, data_release=self.data_release)

            if result is None or len(result) == 0:
                log_message(f"No SDSS object found for plate={plate}, mjd={mjd}, fiberID={fiberid}", print_console=False)
                return None

            row = result[0]
            ra = row["ra"]
            dec = row["dec"]

            log_message(f"Found RA={ra}, DEC={dec} for plate={plate}, mjd={mjd}, fiberID={fiberid}", print_console=False)
            target = self.resolve_target(ra, dec, select=select)
            
            if target and self.cache_results:
                self._cache[cache_key] = target.copy()
                
            return target
        except Exception as e:
            handle_exception("resolve_by_pmfs", e, print_console=False)
            return None
            
    def clear_cache(self):
        cache_size = len(self._cache)
        self._cache = {}
        log_message(f"Cleared resolver cache ({cache_size} entries)")
