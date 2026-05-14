import os
import time
import requests
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
from astropy.io import fits
from astropy.wcs import WCS
from astropy.coordinates import SkyCoord
from astropy.nddata import Cutout2D
from astropy.stats import sigma_clipped_stats
from astropy.visualization import (
    ImageNormalize, AsinhStretch, LinearStretch,
    ManualInterval, PercentileInterval, ZScaleInterval,
)
import astropy.units as u

try:
    from reproject import reproject_interp
    _HAVE_REPROJECT = True
except Exception:
    _HAVE_REPROJECT = False

from .cutout import CutoutFetcher
from .utils import log_message, handle_exception

_LOTSS_BASE = "https://lofar-surveys.org"
_SKYSERVER_MAX_PIX = 2048


def fetch_lotss_cutout(ra: float, dec: float, size_arcmin: float,
                       release: str = "dr2", lowres: bool = False,
                       cache_dir: str = "cache_lotss",
                       max_retries: int = 2, retry_wait: int = 10,
                       timeout: int = 60, verbose: bool = True):
    os.makedirs(cache_dir, exist_ok=True)
    rel = release.lower()
    if rel not in ("dr2", "dr3"):
        raise ValueError(f"release must be 'dr2' or 'dr3', got '{release}'")

    tag = "low" if lowres else "high"
    fname = f"lotss_{rel}_{tag}_ra{ra:.5f}_dec{dec:+.5f}_sz{size_arcmin:.3f}.fits"
    path = os.path.join(cache_dir, fname)

    if os.path.exists(path):
        try:
            return fits.open(path), path
        except Exception:
            os.remove(path)

    suffix = "-low-cutout.fits" if lowres else "-cutout.fits"
    url = f"{_LOTSS_BASE}/{rel}{suffix}"
    params = {"pos": f"{ra:.6f} {dec:+.6f}", "size": float(size_arcmin)}

    for attempt in range(1, max_retries + 2):
        try:
            if verbose:
                log_message(f"LoTSS GET pos={params['pos']} size={params['size']} (attempt {attempt})")
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 404:
                raise RuntimeError(f"LoTSS {rel} out of coverage (HTTP 404) at RA={ra}, DEC={dec}")
            r.raise_for_status()
            content = r.content
            if not content[:30].decode("ascii", errors="ignore").startswith("SIMPLE"):
                raise RuntimeError(f"LoTSS response is not a FITS file (likely out of footprint)")

            with open(path, "wb") as f:
                f.write(content)
            if verbose:
                log_message(f"Saved LoTSS FITS: {path}")
            return fits.open(BytesIO(content)), path
        except Exception as e:
            if verbose:
                log_message(f"LoTSS fetch failed on attempt {attempt}: {e}")
            if attempt <= max_retries:
                time.sleep(retry_wait)

    handle_exception("fetch_lotss_cutout", Exception(f"All {max_retries} attempts failed"))
    return None, ""


class SDSSRadioPlotter:
    """
    SDSSRadioPlotter
    ----------------
    Plots SDSS optical cutouts with LoTSS DR2/DR3 radio contours overlaid.

    Parameters:
    - sdss_dr (int): SDSS data release to use (default: 16)
    - sdss_band (str): SDSS band for the optical FITS (default: 'r')
    - sdss_scale (float): SDSS arcseconds per pixel (default: 0.2)
    - lotss_release (str): LoTSS release, 'dr2' or 'dr3' (default: 'dr2')
    - lotss_lowres (bool): Use low-resolution (20") LoTSS endpoint (default: False)
    - cache_dir (str): Directory for FITS cache (default: 'cache_sdss_radio')
    - figure_dir (str): Directory to save PNG figures (default: 'figures')
    - contour_color (str): Color of the LoTSS contours (default: 'red')
    - n_contour_levels (int): Number of log-spaced contour levels (default: 6)
    - sdss_n_sigma_dark (float): Sigma multiplier for SDSS hard contrast (default: 12.0)
    - cmap_optical (str): Matplotlib colormap for optical (default: 'gray_r')
    - cmap_radio (str): Matplotlib colormap for radio (default: 'inferno')

    Methods:
    - plot(ra, dec, size_arcmin, mode): Make a single figure
    - fetch_sdss(ra, dec, size_arcmin): Download SDSS FITS centered on target
    - fetch_lotss(ra, dec, size_arcmin): Download LoTSS FITS
    """

    def __init__(self, sdss_dr: int = 16, sdss_band: str = "r", sdss_scale: float = 0.2,
                 lotss_release: str = "dr2", lotss_lowres: bool = False,
                 cache_dir: str = "cache_sdss_radio", figure_dir: str = "figures",
                 contour_color: str = "red", contour_linewidth: float = 1.0,
                 n_contour_levels: int = 6, sdss_n_sigma_dark: float = 12.0,
                 align_target_scale_arcsec: float = 0.4,
                 cmap_optical: str = "gray_r", cmap_radio: str = "inferno",
                 verbose: bool = True):
        self.sdss_dr = sdss_dr
        self.sdss_band = sdss_band
        self.sdss_scale = sdss_scale
        self.lotss_release = lotss_release.lower()
        self.lotss_lowres = lotss_lowres
        self.cache_dir = cache_dir
        self.figure_dir = figure_dir
        self.contour_color = contour_color
        self.contour_linewidth = contour_linewidth
        self.n_contour_levels = n_contour_levels
        self.sdss_n_sigma_dark = sdss_n_sigma_dark
        self.align_target_scale_arcsec = align_target_scale_arcsec
        self.cmap_optical = cmap_optical
        self.cmap_radio = cmap_radio
        self.verbose = verbose

        self._sdss_jpeg_dir = os.path.join(cache_dir, "sdss_jpeg")
        self._sdss_fits_dir = os.path.join(cache_dir, "sdss_fits")
        self._lotss_dir = os.path.join(cache_dir, "lotss")
        for d in (cache_dir, figure_dir, self._sdss_jpeg_dir,
                  self._sdss_fits_dir, self._lotss_dir):
            os.makedirs(d, exist_ok=True)

    def fetch_sdss(self, ra: float, dec: float, size_arcmin: float) -> str:
        size_pix = int(round((size_arcmin * 60.0) / self.sdss_scale))
        scale = self.sdss_scale
        if size_pix > _SKYSERVER_MAX_PIX:
            scale = (size_arcmin * 60.0) / _SKYSERVER_MAX_PIX
            size_pix = _SKYSERVER_MAX_PIX

        cutout = CutoutFetcher(
            output_dir=self._sdss_jpeg_dir, scale=scale, size=size_pix,
            opt="", data_release=self.sdss_dr,
        )
        try:
            parent_fits = cutout.fetch_fits_image(band=self.sdss_band, ra=ra, dec=dec)
        except Exception as e:
            handle_exception("fetch_sdss", e)
            return ""
        if not parent_fits:
            return ""

        out_name = f"sdss_dr{self.sdss_dr}_{self.sdss_band}_ra{ra:.5f}_dec{dec:+.5f}_sz{size_arcmin:.3f}.fits"
        out_path = os.path.join(self._sdss_fits_dir, out_name)
        cropped = self._crop_centered(parent_fits, ra, dec, size_arcmin, out_path)
        return cropped if cropped else parent_fits

    def fetch_lotss(self, ra: float, dec: float, size_arcmin: float):
        hdul, _ = fetch_lotss_cutout(
            ra=ra, dec=dec, size_arcmin=size_arcmin,
            release=self.lotss_release, lowres=self.lotss_lowres,
            cache_dir=self._lotss_dir, verbose=self.verbose,
        )
        if hdul is None:
            return None, None
        try:
            hdu = None
            for cand in hdul:
                if cand.data is not None and cand.data.ndim >= 2:
                    hdu = cand
                    break
            if hdu is None:
                return None, None
            data = self._squeeze_2d(hdu.data)
            wcs = self._celestial_wcs(hdu.header)
            return data, wcs
        finally:
            hdul.close()

    def plot(self, ra: float, dec: float, size_arcmin: float = 3.0,
             mode: str = "both", save_path: str = None, show: bool = True,
             rms: float = None, figsize: tuple = (8, 8),
             title: str = None, show_target_marker: bool = True) -> str:
        mode = mode.lower()
        if mode not in ("both", "optical", "radio"):
            log_message(f"Invalid mode: {mode}. Use 'both', 'optical', or 'radio'.")
            return ""

        sdss_fits = ""
        lotss_data, lotss_wcs = None, None

        if mode in ("both", "optical"):
            sdss_fits = self.fetch_sdss(ra, dec, size_arcmin)
            if not sdss_fits and mode == "optical":
                log_message("No SDSS FITS available; skipping plot.")
                return ""

        if mode in ("both", "radio"):
            try:
                lotss_data, lotss_wcs = self.fetch_lotss(ra, dec, size_arcmin)
            except Exception as e:
                handle_exception("plot", e)
                if mode == "radio":
                    return ""

        if rms is None and lotss_data is not None:
            rms = self._estimate_rms(lotss_data)

        fig = plt.figure(figsize=figsize)

        if mode == "optical":
            return self._plot_optical(fig, ra, dec, sdss_fits, save_path,
                                      show, title, show_target_marker)
        if mode == "radio":
            return self._plot_radio(fig, ra, dec, lotss_data, lotss_wcs, rms,
                                    save_path, show, title, show_target_marker)
        return self._plot_both(fig, ra, dec, size_arcmin, sdss_fits,
                               lotss_data, lotss_wcs, rms,
                               save_path, show, title, show_target_marker)

    def _plot_both(self, fig, ra, dec, size_arcmin, sdss_fits,
                   lotss_data, lotss_wcs, rms,
                   save_path, show, title, show_target_marker):
        sdss_arr, proj_wcs = self._load_sdss(sdss_fits)
        radio_arr = None

        if _HAVE_REPROJECT and sdss_arr is not None and lotss_data is not None:
            common_wcs, shape_out = self._north_up_wcs(ra, dec, size_arcmin)
            try:
                sdss_proj, _ = reproject_interp((sdss_arr, proj_wcs),
                                                common_wcs, shape_out=shape_out)
                radio_proj, _ = reproject_interp((lotss_data, lotss_wcs),
                                                 common_wcs, shape_out=shape_out)
                proj_wcs = common_wcs
                sdss_arr = sdss_proj
                radio_arr = radio_proj
            except Exception as e:
                handle_exception("reproject", e)
        elif _HAVE_REPROJECT and sdss_arr is not None and lotss_data is not None:
            try:
                radio_arr, _ = reproject_interp((lotss_data, lotss_wcs),
                                                proj_wcs, shape_out=sdss_arr.shape)
            except Exception as e:
                handle_exception("reproject_lotss", e)

        if proj_wcs is None and lotss_wcs is not None:
            proj_wcs = lotss_wcs
            radio_arr = lotss_data

        ax = fig.add_subplot(111, projection=proj_wcs)
        if sdss_arr is not None:
            ax.imshow(sdss_arr, origin="lower", cmap=self.cmap_optical,
                      norm=self._sdss_norm(sdss_arr))

        if radio_arr is not None and rms is not None:
            levels = self._contour_levels(rms)
            try:
                ax.contour(radio_arr, levels=levels, colors=self.contour_color,
                           linewidths=self.contour_linewidth)
                ax.contour(radio_arr, levels=[-3.0 * rms], colors="gray",
                           linewidths=0.6, linestyles="dashed")
            except Exception as e:
                handle_exception("contour", e)

        self._draw_marker(ax, ra, dec, show_target_marker)
        self._set_axes(ax)
        ax.set_title(title or
                     f"SDSS DR{self.sdss_dr} {self.sdss_band}-band + "
                     f"LoTSS {self.lotss_release.upper()} contours")
        return self._save(fig, ra, dec, save_path, show, "sdss_radio")

    def _plot_optical(self, fig, ra, dec, sdss_fits,
                      save_path, show, title, show_target_marker):
        sdss_arr, wcs = self._load_sdss(sdss_fits)
        if sdss_arr is None:
            log_message("No SDSS FITS; plot skipped.")
            plt.close(fig)
            return ""
        ax = fig.add_subplot(111, projection=wcs)
        ax.imshow(sdss_arr, origin="lower", cmap=self.cmap_optical,
                  norm=self._sdss_norm(sdss_arr))
        self._draw_marker(ax, ra, dec, show_target_marker)
        self._set_axes(ax)
        ax.set_title(title or f"SDSS DR{self.sdss_dr} {self.sdss_band}-band")
        return self._save(fig, ra, dec, save_path, show, "sdss_only")

    def _plot_radio(self, fig, ra, dec, lotss_data, lotss_wcs, rms,
                    save_path, show, title, show_target_marker):
        if lotss_data is None or lotss_wcs is None:
            log_message("No LoTSS data; plot skipped.")
            plt.close(fig)
            return ""
        ax = fig.add_subplot(111, projection=lotss_wcs)
        ax.imshow(lotss_data, origin="lower", cmap=self.cmap_radio,
                  norm=self._radio_norm(lotss_data))
        if rms is not None:
            try:
                ax.contour(lotss_data, levels=self._contour_levels(rms),
                           colors="white", linewidths=0.7, alpha=0.7)
            except Exception as e:
                handle_exception("contour", e)
        self._draw_marker(ax, ra, dec, show_target_marker)
        self._set_axes(ax)
        ax.set_title(title or f"LoTSS {self.lotss_release.upper()}"
                     + (" (low-res)" if self.lotss_lowres else ""))
        return self._save(fig, ra, dec, save_path, show, "lotss_only")

    def _load_sdss(self, fits_path: str):
        if not fits_path or not os.path.exists(fits_path):
            return None, None
        with fits.open(fits_path) as hdul:
            hdu = hdul[0]
            if hdu.data is None and len(hdul) > 1:
                hdu = hdul[1]
            data = self._squeeze_2d(hdu.data)
            wcs = self._celestial_wcs(hdu.header)
        return data, wcs

    def _crop_centered(self, parent_path, ra, dec, size_arcmin, out_path):
        try:
            with fits.open(parent_path) as hdul:
                hdu = None
                for cand in hdul:
                    if cand.data is not None and cand.data.ndim >= 2:
                        hdu = cand
                        break
                if hdu is None:
                    return ""
                data = self._squeeze_2d(hdu.data)
                wcs = self._celestial_wcs(hdu.header)
                center = SkyCoord(ra * u.deg, dec * u.deg, frame="icrs")
                cut = Cutout2D(data, position=center,
                               size=(size_arcmin * u.arcmin, size_arcmin * u.arcmin),
                               wcs=wcs, mode="partial", fill_value=np.nan, copy=True)
                new_header = cut.wcs.to_header()
                for k in ("BUNIT", "FILTER", "DATE-OBS", "MJD-OBS",
                          "TELESCOP", "INSTRUME", "EQUINOX", "RADESYS"):
                    if k in hdu.header:
                        new_header[k] = hdu.header[k]
                fits.PrimaryHDU(data=cut.data, header=new_header).writeto(out_path, overwrite=True)
                return out_path
        except Exception as e:
            handle_exception("_crop_centered", e)
            return ""

    def _north_up_wcs(self, ra, dec, size_arcmin):
        size_pix = max(1, int(round(size_arcmin * 60.0 / self.align_target_scale_arcsec)))
        w = WCS(naxis=2)
        w.wcs.ctype = ["RA---TAN", "DEC--TAN"]
        w.wcs.crval = [ra, dec]
        w.wcs.crpix = [size_pix / 2.0 + 0.5, size_pix / 2.0 + 0.5]
        w.wcs.cdelt = [-self.align_target_scale_arcsec / 3600.0,
                        self.align_target_scale_arcsec / 3600.0]
        w.wcs.cunit = ["deg", "deg"]
        w.wcs.radesys = "ICRS"
        return w, (size_pix, size_pix)

    def _contour_levels(self, rms):
        return np.asarray(3.0 * (np.sqrt(2) ** np.arange(self.n_contour_levels))) * rms

    def _estimate_rms(self, data):
        finite = data[np.isfinite(data)]
        if finite.size == 0:
            return 1e-6
        _, _, std = sigma_clipped_stats(finite, sigma=3.0, maxiters=3)
        return float(std) if std > 0 else 1e-6

    def _sdss_norm(self, data):
        finite = data[np.isfinite(data)]
        if finite.size == 0:
            return ImageNormalize()
        try:
            _, median, std = sigma_clipped_stats(finite, sigma=3.0, maxiters=3)
            vmin = float(median)
            vmax = float(median + self.sdss_n_sigma_dark * std)
            if not np.isfinite(vmax) or vmax <= vmin:
                vmax = vmin + 1.0
            return ImageNormalize(data, interval=ManualInterval(vmin=vmin, vmax=vmax),
                                  stretch=LinearStretch())
        except Exception:
            return ImageNormalize(data, interval=ZScaleInterval(), stretch=LinearStretch())

    def _radio_norm(self, data):
        finite = data[np.isfinite(data)]
        if finite.size == 0:
            return ImageNormalize()
        return ImageNormalize(data, interval=PercentileInterval(99.5), stretch=AsinhStretch())

    def _draw_marker(self, ax, ra, dec, enabled):
        if not enabled:
            return
        try:
            ax.plot(ra, dec, marker="x", color="cyan", markersize=14, mew=2.5,
                    transform=ax.get_transform("world"), zorder=10)
        except Exception:
            pass

    def _set_axes(self, ax):
        try:
            ax.coords[0].set_axislabel("RA (J2000)")
            ax.coords[1].set_axislabel("Dec (J2000)")
            ax.coords.grid(color="0.7", ls=":", alpha=0.4)
        except Exception:
            pass

    def _save(self, fig, ra, dec, save_path, show, prefix):
        if not save_path:
            save_path = os.path.join(self.figure_dir,
                                     f"{prefix}_ra{ra:.4f}_dec{dec:+.4f}.png")
        try:
            fig.savefig(save_path, dpi=120, bbox_inches="tight")
            log_message(f"Saved figure: {save_path}")
        except Exception as e:
            handle_exception("_save", e)
            save_path = ""
        if show:
            plt.show()
        else:
            plt.close(fig)
        return save_path

    @staticmethod
    def _squeeze_2d(arr):
        arr = np.asarray(arr)
        while arr.ndim > 2:
            arr = arr[0]
        return arr

    @staticmethod
    def _celestial_wcs(header):
        w = WCS(header)
        return w.celestial if w.naxis > 2 else w


def sdss_radio(ra: float, dec: float, size_arcmin: float = 3.0,
               mode: str = "both", **kwargs) -> str:
    plot_keys = {"save_path", "show", "rms", "figsize", "title", "show_target_marker"}
    plot_kwargs = {k: kwargs.pop(k) for k in list(kwargs) if k in plot_keys}
    plotter = SDSSRadioPlotter(**kwargs)
    return plotter.plot(ra=ra, dec=dec, size_arcmin=size_arcmin, mode=mode, **plot_kwargs)
