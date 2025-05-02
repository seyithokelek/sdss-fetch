import os
import pandas as pd
import numpy as np
from astropy.io import fits
from astropy.table import Table
import matplotlib.pyplot as plt
from .utils import log_message, handle_exception, safe_to_csv, safe_to_fits

class MetadataExtractor:
    """
    metadata.py - SDSS Spectral Metadata Module
    -------------------------------------------

    Provides methods to extract summary information and emission-line properties
    from SDSS FITS spectrum files, and to visualize rest-frame spectra.

    Main Features:
    - Extract plate, mjd, fiberID, redshift (z), object class, SNR, specobjid
    - Summarize detected emission lines with flux and SNR
    - Plot rest-frame spectrum with ±1σ shaded region and emission line overlays
    
    Parameters:
    - cache_results (bool): Whether to cache results for repeated calls (default: True)
    """

    def __init__(self, cache_results: bool = True):
        self.cache_results = cache_results
        self._cache = {}

    def extract_metadata(self, file_list):
        records = []
        for f in file_list:
            if self.cache_results and f in self._cache.get('metadata', {}):
                records.append(self._cache['metadata'][f])
                log_message(f"Using cached metadata for {f}")
                continue
                
            try:
                with fits.open(f) as hdul:
                    hdr = Table(hdul[2].data)

                    sn = np.nan
                    if "SN_MEDIAN_ALL" in hdr.colnames:
                        sn = float(hdr["SN_MEDIAN_ALL"][0])
                    elif "SN_MEDIAN" in hdr.colnames:
                        sn_median = hdr["SN_MEDIAN"][0]
                        sn = float(np.mean(sn_median)) if hasattr(sn_median, '__len__') else float(sn_median)

                    record = {
                        "filename": os.path.basename(f),
                        "plate": int(hdr["PLATE"][0]),
                        "mjd": int(hdr["MJD"][0]),
                        "fiberID": int(hdr["FIBERID"][0]),
                        "z": float(hdr["Z"][0]),
                        "z_err": float(hdr["Z_ERR"][0]),
                        "class": hdr["CLASS"][0].strip(),
                        "subclass": hdr["SUBCLASS"][0].strip(),
                        "snr": sn,
                        "specobjid": str(hdr["SPECOBJID"][0]),
                    }
                    
                    records.append(record)
                    
                    if self.cache_results:
                        if 'metadata' not in self._cache:
                            self._cache['metadata'] = {}
                        self._cache['metadata'][f] = record.copy()
            except Exception as e:
                handle_exception("extract_metadata", e)
                records.append({"filename": os.path.basename(f),
                                "plate": None, "mjd": None, "fiberID": None,
                                "z": None, "z_err": None, "class": None, "subclass": None,
                                "snr": None, "specobjid": None})
        return pd.DataFrame.from_records(records)

    def summarize_lines(self, filepath):
        if self.cache_results and filepath in self._cache.get('lines', {}):
            log_message(f"Using cached line data for {filepath}")
            return self._cache['lines'][filepath]
            
        try:
            with fits.open(filepath) as hdul:
                data = hdul[3].data
                lines = data[data["LINEAREA"] > 0]
                df = pd.DataFrame({
                    "linename": lines["LINENAME"].astype(str),
                    "linewave": lines["LINEWAVE"],
                    "sigma": lines["LINESIGMA"],
                    "flux": lines["LINEAREA"],
                    "snr": lines["LINEAREA"] / lines["LINEAREA_ERR"]
                })
                
                if self.cache_results:
                    if 'lines' not in self._cache:
                        self._cache['lines'] = {}
                    self._cache['lines'][filepath] = df.copy()
                    
                return df
        except Exception as e:
            handle_exception("summarize_lines", e)
            return pd.DataFrame()

    def plot_restframe(self, filepath, show_lines=True, xrange=None, yrange=None, grid=False, save_fig=False, fig_path=None):
        try:
            with fits.open(filepath) as hdul:
                flux = hdul[1].data["FLUX"]
                loglam = hdul[1].data["LOGLAM"]
                ivar = hdul[1].data["IVAR"]
                lam = 10 ** loglam

                hdr = Table(hdul[2].data)
                z = hdr["Z"][0]
                plate = hdr["PLATE"][0]
                mjd = hdr["MJD"][0]
                fiber = hdr["FIBERID"][0]

                rest_lam = lam / (1 + z)
                rest_flux = flux * (1 + z)
                err = np.sqrt(1 / np.where(ivar > 0, ivar, 1e-10))
                rest_err = err * (1 + z)
                mask = rest_flux > (3 * rest_err)
                rest_fluxcs = rest_flux[mask]

                fig, ax = plt.subplots(figsize=(10, 6))

                ax.plot(rest_lam, rest_flux, label=f"z = {z:.4f}", lw=1.2, zorder=5)
                ax.fill_between(
                    rest_lam,
                    rest_flux - rest_err,
                    rest_flux + rest_err,
                    color='gray',
                    alpha=0.3,
                    label='±1σ',
                    zorder=4
                )

                if show_lines:
                    emission_lines = {
                        1033.82: 'O VI', 1215.24: 'Ly α', 1240.81: 'N V', 1305.53: 'O I', 1335.31: 'C II',
                        1397.61: 'Si IV', 1399.80: 'Si IV + O IV', 1549.48: 'C IV', 1640.40: 'He II',
                        1665.85: 'O III]', 1857.40: 'Al III', 1908.73: 'C III]', 2326.00: 'C II]',
                        2439.50: 'Ne IV', 2799.12: 'Mg II', 3346.79: 'Ne V', 3426.85: 'Ne VI',
                        3727.09: '[O II]', 3729.88: '[O II]', 3889.00: 'He I', 4072.30: 'S II',
                        4102.89: 'H δ', 4341.68: 'H γ', 4364.44: '[O III]', 4862.68: 'H β',
                        4932.60: '[O III]', 4960.30: '[O III]', 5008.24: '[O III]', 6302.05: '[O I]',
                        6365.54: '[O I]', 6529.03: '[N I]', 6549.86: '[N II]', 6564.61: 'H α',
                        6585.27: '[N II]', 6718.29: '[S II]', 6732.67: '[S II]'
                    }

                    absorption_lines = {
                        3934.78: 'Ca II K', 3969.59: 'Ca II H', 4305.61: 'G band', 5176.70: 'Mg',
                        5895.60: 'Na', 8500.36: 'Ca II', 8544.44: 'Ca II', 8664.52: 'Ca II'
                    }

                    sky_lines = {
                        5578.50: 'Sky', 5894.60: 'Sky', 6301.70: 'Sky', 7246.00: 'Sky'
                    }

                    for wl, name in emission_lines.items():
                        if rest_lam.min() < wl < rest_lam.max():
                            ax.axvline(x=wl, color='red', linestyle='--', lw=0.6, zorder=3)
                            ax.text(wl + 7, rest_fluxcs.max() * 0.85, name,
                                    rotation=90, fontsize=8, va='top', color='red', zorder=3)

                    for wl, name in absorption_lines.items():
                        if rest_lam.min() < wl < rest_lam.max():
                            ax.axvline(x=wl, color='blue', linestyle='--', lw=0.6, zorder=3)
                            ax.text(wl + 7, rest_fluxcs.max() * 0.85, name,
                                    rotation=90, fontsize=8, va='top', color='blue', zorder=3)

                    for wl, name in sky_lines.items():
                        if rest_lam.min() < wl < rest_lam.max():
                            ax.axvline(x=wl, color='gold', linestyle='--', lw=0.6, zorder=3)
                            ax.text(wl + 7, rest_fluxcs.max() * 0.85, name,
                                    rotation=90, fontsize=8, va='top', color='gold', zorder=3)

                if xrange is None:
                    xrange = (rest_lam.min() - 50, rest_lam.max() + 50)
                if yrange is None:
                    yrange = (rest_fluxcs.min() - 5, rest_fluxcs.max() + 5)

                ax.set_xlim(xrange)
                ax.set_ylim(yrange)

                ax.set_xlabel(r'Rest-frame Wavelength (\AA)', fontsize=13)
                ax.set_ylabel(r'$f_\lambda$ ($10^{-17}$ erg s$^{-1}$ cm$^{-2}$ \AA$^{-1}$)', fontsize=13)
                ax.set_title(f"spec-{plate}-{mjd}-{fiber} (rest-frame)")
                ax.legend(loc='upper right', fontsize=9)

                if grid:
                    ax.grid(True, linestyle='--', alpha=0.5)

                fig.tight_layout()
                
                if save_fig:
                    if fig_path is None:
                        base_name = os.path.splitext(os.path.basename(filepath))[0]
                        fig_path = f"{base_name}_restframe.png"
                    
                    plt.savefig(fig_path, dpi=300, bbox_inches='tight')
                    log_message(f"Saved rest-frame plot to {fig_path}")
                
                plt.show()
                return fig
        except Exception as e:
            handle_exception("plot_restframe", e)
            print(f"Plot failed for {filepath}: {e}")
            return None
            
    def save_metadata_to_csv(self, metadata_df, filename="metadata.csv"):
        try:
            safe_to_csv(metadata_df, filename)
        except Exception as e:
            handle_exception("save_metadata_to_csv", e)
            
    def save_lines_to_csv(self, lines_df, filename="emission_lines.csv"):
        try:
            safe_to_csv(lines_df, filename)
        except Exception as e:
            handle_exception("save_lines_to_csv", e)
            
    def clear_cache(self):
        cache_size = sum(len(cache) for cache in self._cache.values())
        self._cache = {}
        log_message(f"Cleared metadata cache ({cache_size} entries)")
