"""
sdss_radio
----------
SDSS optik + LoTSS DR2/DR3 radyo kontur cizici.

radio_morphology paketinden bagimsiz, SINIFLANDIRMA / JET / GRQ / FR1-FR2
ANALIZI YAPMAYAN sade bir gorsellestirme modulu. Sadece:

    1) SDSS r-band FITS cutout indir
    2) LoTSS DR2/DR3 FITS cutout indir
    3) Iki goruntuyu ortak kuzey-yukari WCS'e reproject et
    4) SDSS uzerine LoTSS kontur cizimi yap (ekteki ornek figur gibi)

API:
    from sdss_fetch import SDSSRadioPlotter, sdss_radio

    # Sinif kullanimi (parametre tutmak istersen)
    plotter = SDSSRadioPlotter(lotss_release="dr2")
    plotter.plot(ra=187.27, dec=2.05, size_arcmin=3.0)              # SDSS + radyo
    plotter.plot(ra=..., dec=..., size_arcmin=2.0, mode="optical")  # sadece SDSS
    plotter.plot(ra=..., dec=..., size_arcmin=3.0, mode="radio")    # sadece LoTSS

    # Tek seferlik kisa-yol
    sdss_radio(ra=187.27, dec=2.05, size_arcmin=3.0)

Modlar:
    'both'    (default) - SDSS r-band + LoTSS kontur (ekteki ornek)
    'optical'           - Sadece SDSS r-band
    'radio'             - Sadece LoTSS radyo haritasi
"""
from __future__ import annotations

import os
import time
from io import BytesIO
from typing import Optional, Tuple

import numpy as np
import requests
import matplotlib.pyplot as plt

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
from .utils import log_message


_LOTSS_BASE = "https://lofar-surveys.org"
SKYSERVER_MAX_PIX = 2048


# ====================================================================
# LoTSS fetch (radio_morphology.lotss'in sade ozeti, bagimsiz)
# ====================================================================
def _lotss_url(release: str, lowres: bool) -> str:
    release = release.lower()
    if release not in ("dr2", "dr3"):
        raise ValueError(f"release must be 'dr2' or 'dr3', got '{release}'")
    suffix = "-low-cutout.fits" if lowres else "-cutout.fits"
    return f"{_LOTSS_BASE}/{release}{suffix}"


def fetch_lotss_cutout(ra: float, dec: float, size_arcmin: float,
                       release: str = "dr2", lowres: bool = False,
                       cache_dir: str = "cache_lotss",
                       max_retries: int = 2, retry_wait: int = 10,
                       timeout: int = 60, verbose: bool = True
                       ) -> Tuple[fits.HDUList, str]:
    """LoTSS FITS cutout indir (cache'li). (HDUList, path) dondurur."""
    os.makedirs(cache_dir, exist_ok=True)
    rel = release.lower()
    tag = "low" if lowres else "high"
    fname = (f"lotss_{rel}_{tag}_ra{ra:.5f}_dec{dec:+.5f}"
             f"_sz{size_arcmin:.3f}.fits")
    path = os.path.join(cache_dir, fname)

    if os.path.exists(path):
        try:
            return fits.open(path), path
        except Exception:
            try:
                os.remove(path)
            except Exception:
                pass

    url = _lotss_url(rel, lowres)
    params = {"pos": f"{ra:.6f} {dec:+.6f}", "size": float(size_arcmin)}

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 2):
        try:
            if verbose:
                log_message(
                    f"[LoTSS] GET {url} pos={params['pos']} "
                    f"size={params['size']} arcmin (attempt {attempt})"
                )
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 404:
                raise RuntimeError(
                    f"LoTSS {rel} coverage out (HTTP 404) "
                    f"RA={ra}, Dec={dec}"
                )
            r.raise_for_status()
            content = r.content
            head = content[:30].decode("ascii", errors="ignore")
            if not head.startswith("SIMPLE"):
                snippet = content[:120].decode("ascii", errors="replace")
                raise RuntimeError(
                    f"LoTSS response is not FITS "
                    f"(probably out of footprint): {snippet!r}"
                )
            with open(path, "wb") as f:
                f.write(content)
            if verbose:
                log_message(f"[LoTSS] saved -> {path}")
            return fits.open(BytesIO(content)), path
        except Exception as e:
            last_exc = e
            if verbose:
                log_message(f"[LoTSS] attempt {attempt} failed: {e}")
            if attempt <= max_retries:
                time.sleep(retry_wait)

    raise RuntimeError(
        f"LoTSS fetch failed RA={ra}, Dec={dec}: {last_exc}"
    )


# ====================================================================
# WCS / norm yardimcilari
# ====================================================================
def _celestial_wcs(header) -> WCS:
    w = WCS(header)
    return w.celestial if w.naxis > 2 else w


def _squeeze_2d(arr) -> np.ndarray:
    arr = np.asarray(arr)
    while arr.ndim > 2:
        arr = arr[0]
    return arr


def _make_north_up_wcs(ra: float, dec: float, size_arcmin: float,
                       scale_arcsec_per_pix: float = 0.4):
    size_pix = max(1, int(round(size_arcmin * 60.0 / scale_arcsec_per_pix)))
    w = WCS(naxis=2)
    w.wcs.ctype = ["RA---TAN", "DEC--TAN"]
    w.wcs.crval = [float(ra), float(dec)]
    w.wcs.crpix = [size_pix / 2.0 + 0.5, size_pix / 2.0 + 0.5]
    w.wcs.cdelt = [-scale_arcsec_per_pix / 3600.0,
                    scale_arcsec_per_pix / 3600.0]
    w.wcs.cunit = ["deg", "deg"]
    w.wcs.radesys = "ICRS"
    return w, (size_pix, size_pix)


def _contour_levels_log(rms: float, n_levels: int = 6,
                        sigma_start: float = 3.0,
                        base: float = np.sqrt(2)) -> np.ndarray:
    return np.asarray(sigma_start * (base ** np.arange(n_levels))) * rms


def _estimate_rms(data) -> float:
    finite = data[np.isfinite(data)]
    if finite.size == 0:
        return 1e-6
    _, _, std = sigma_clipped_stats(finite, sigma=3.0, maxiters=3)
    return float(std) if std > 0 else 1e-6


def _sdss_hard_contrast_norm(data, n_sigma_dark: float = 12.0):
    """SDSS arka plan: median = beyaz, median+N*sigma = siyah (cmap=gray_r)."""
    finite = data[np.isfinite(data)]
    if finite.size == 0:
        return ImageNormalize()
    try:
        _, median, std = sigma_clipped_stats(finite, sigma=3.0, maxiters=3)
        vmin = float(median)
        vmax = float(median + n_sigma_dark * std)
        if not np.isfinite(vmax) or vmax <= vmin:
            vmax = vmin + 1.0
        return ImageNormalize(
            data, interval=ManualInterval(vmin=vmin, vmax=vmax),
            stretch=LinearStretch(),
        )
    except Exception:
        return ImageNormalize(data, interval=ZScaleInterval(),
                              stretch=LinearStretch())


def _radio_norm(data, kind: str = "asinh"):
    finite = data[np.isfinite(data)]
    if finite.size == 0:
        return ImageNormalize()
    if kind == "asinh":
        return ImageNormalize(data, interval=PercentileInterval(99.5),
                              stretch=AsinhStretch())
    return ImageNormalize(data, interval=ZScaleInterval(),
                          stretch=LinearStretch())


def _crop_fits_centered(parent_fits_path: str, ra: float, dec: float,
                        size_arcmin: float, output_path: str
                        ) -> Optional[str]:
    """Parent SDSS framinden hedef-merkezli kirpinti olustur."""
    if not parent_fits_path or not os.path.exists(parent_fits_path):
        return None
    try:
        with fits.open(parent_fits_path) as hdul:
            hdu = None
            for cand in hdul:
                if cand.data is not None and cand.data.ndim >= 2:
                    hdu = cand
                    break
            if hdu is None:
                return None
            data = _squeeze_2d(hdu.data)
            wcs = _celestial_wcs(hdu.header)
            center = SkyCoord(ra * u.deg, dec * u.deg, frame="icrs")
            size = (size_arcmin * u.arcmin, size_arcmin * u.arcmin)
            cut = Cutout2D(
                data, position=center, size=size, wcs=wcs,
                mode="partial", fill_value=np.nan, copy=True,
            )
            new_h = cut.wcs.to_header()
            for k in ("BUNIT", "BMAJ", "BMIN", "BPA", "TELESCOP",
                      "INSTRUME", "FILTER", "DATE-OBS", "MJD-OBS",
                      "EQUINOX", "RADESYS"):
                if k in hdu.header:
                    new_h[k] = hdu.header[k]
            fits.PrimaryHDU(data=cut.data, header=new_h).writeto(
                output_path, overwrite=True)
            return output_path
    except Exception as e:
        log_message(f"[sdss_radio] crop hata: {e}")
        return None


# ====================================================================
# Ana sinif
# ====================================================================
class SDSSRadioPlotter:
    """SDSS optik + LoTSS radyo kontur cizici (sade, sinif yok).

    Parameters
    ----------
    sdss_dr : int
        SDSS data release (default 16).
    sdss_band : str
        SDSS bandi (default 'r').
    sdss_scale : float
        SDSS arcsec/pixel (default 0.2).
    lotss_release : str
        'dr2' (default) veya 'dr3'.
    lotss_lowres : bool
        True ise low-res (20") LoTSS endpoint.
    cache_dir : str
        FITS cache dizini (alt klasorler: sdss_jpeg, sdss_fits, lotss).
    figure_dir : str
        PNG cikti dizini.
    contour_color : str
        Kontur rengi (default 'red').
    contour_linewidth : float
        Kontur kalinligi (default 1.0).
    n_contour_levels : int
        Log kontur seviye sayisi (default 6 -> 3, 3*sqrt2, 6, ... * RMS).
    sdss_n_sigma_dark : float
        SDSS sert kontrast siniri (median+N*sigma kararir; default 12.0).
    align_target_scale_arcsec : float
        Ortak grid icin pixel olcegi (default 0.4 arcsec/pix).
    cmap_optical, cmap_radio : str
        Renk haritalari ('gray_r', 'inferno').
    verbose : bool
        Konsola log yaz.
    """

    def __init__(
        self,
        sdss_dr: int = 16,
        sdss_band: str = "r",
        sdss_scale: float = 0.2,
        lotss_release: str = "dr2",
        lotss_lowres: bool = False,
        cache_dir: str = "cache_sdss_radio",
        figure_dir: str = "figures",
        contour_color: str = "red",
        contour_linewidth: float = 1.0,
        n_contour_levels: int = 6,
        sdss_n_sigma_dark: float = 12.0,
        align_target_scale_arcsec: float = 0.4,
        cmap_optical: str = "gray_r",
        cmap_radio: str = "inferno",
        verbose: bool = True,
    ):
        self.sdss_dr = int(sdss_dr)
        self.sdss_band = str(sdss_band)
        self.sdss_scale = float(sdss_scale)
        self.lotss_release = lotss_release.lower()
        self.lotss_lowres = bool(lotss_lowres)
        self.cache_dir = cache_dir
        self.figure_dir = figure_dir
        self.contour_color = contour_color
        self.contour_linewidth = float(contour_linewidth)
        self.n_contour_levels = int(n_contour_levels)
        self.sdss_n_sigma_dark = float(sdss_n_sigma_dark)
        self.align_target_scale_arcsec = float(align_target_scale_arcsec)
        self.cmap_optical = cmap_optical
        self.cmap_radio = cmap_radio
        self.verbose = bool(verbose)

        self._sdss_jpeg_dir = os.path.join(cache_dir, "sdss_jpeg")
        self._sdss_fits_dir = os.path.join(cache_dir, "sdss_fits")
        self._lotss_dir = os.path.join(cache_dir, "lotss")
        for d in (cache_dir, figure_dir, self._sdss_jpeg_dir,
                  self._sdss_fits_dir, self._lotss_dir):
            os.makedirs(d, exist_ok=True)

    # -----------------------------------------------------------------
    def _log(self, msg: str) -> None:
        if self.verbose:
            log_message(f"[SDSSRadioPlotter] {msg}")

    # -----------------------------------------------------------------
    # SDSS fetch (sdss_fetch.CutoutFetcher araciligiyla)
    # -----------------------------------------------------------------
    def _fetch_sdss_fits(self, ra: float, dec: float,
                         size_arcmin: float) -> Optional[str]:
        size_pix = int(round((size_arcmin * 60.0) / self.sdss_scale))
        scale = self.sdss_scale
        if size_pix > SKYSERVER_MAX_PIX:
            scale = (size_arcmin * 60.0) / SKYSERVER_MAX_PIX
            size_pix = SKYSERVER_MAX_PIX

        cutout = CutoutFetcher(
            output_dir=self._sdss_jpeg_dir,
            scale=scale, size=size_pix,
            opt="", data_release=self.sdss_dr,
        )
        try:
            parent_fits = cutout.fetch_fits_image(
                band=self.sdss_band, ra=ra, dec=dec
            )
        except Exception as e:
            self._log(f"SDSS FITS hata: {e}")
            return None
        if not parent_fits:
            return None

        cropped_name = (
            f"sdss_dr{self.sdss_dr}_{self.sdss_band}"
            f"_ra{ra:.5f}_dec{dec:+.5f}_sz{size_arcmin:.3f}.fits"
        )
        cropped_path = os.path.join(self._sdss_fits_dir, cropped_name)
        cropped = _crop_fits_centered(
            parent_fits, ra, dec, size_arcmin, cropped_path
        )
        return cropped if cropped else parent_fits

    # -----------------------------------------------------------------
    # LoTSS fetch
    # -----------------------------------------------------------------
    def _fetch_lotss(self, ra: float, dec: float, size_arcmin: float
                     ) -> Tuple[Optional[np.ndarray], Optional[WCS],
                                Optional[fits.Header]]:
        hdul, _ = fetch_lotss_cutout(
            ra=ra, dec=dec, size_arcmin=size_arcmin,
            release=self.lotss_release, lowres=self.lotss_lowres,
            cache_dir=self._lotss_dir, verbose=self.verbose,
        )
        try:
            hdu = None
            for cand in hdul:
                if cand.data is not None and cand.data.ndim >= 2:
                    hdu = cand
                    break
            if hdu is None:
                return None, None, None
            data = _squeeze_2d(hdu.data)
            wcs = _celestial_wcs(hdu.header)
            header = hdu.header.copy()
            return data, wcs, header
        finally:
            try:
                hdul.close()
            except Exception:
                pass

    # -----------------------------------------------------------------
    # Reproject (her ikisini ayni kuzey-yukari TAN gridine)
    # -----------------------------------------------------------------
    def _reproject_to_common(self, sdss_fits_path: Optional[str],
                             lotss_data: np.ndarray, lotss_wcs: WCS,
                             ra: float, dec: float, size_arcmin: float):
        if not _HAVE_REPROJECT:
            self._log("reproject paketi yok; ortak grid olusturulamiyor.")
            return None
        common_wcs, shape_out = _make_north_up_wcs(
            ra, dec, size_arcmin, self.align_target_scale_arcsec
        )
        sdss_proj = None
        if sdss_fits_path and os.path.exists(sdss_fits_path):
            try:
                with fits.open(sdss_fits_path) as hdul:
                    primary = hdul[0]
                    sdss_data = primary.data
                    sdss_header = primary.header
                    if sdss_data is None and len(hdul) > 1:
                        primary = hdul[1]
                        sdss_data = primary.data
                        sdss_header = primary.header
                if sdss_data is not None:
                    sdss_2d = _squeeze_2d(sdss_data)
                    sdss_wcs = _celestial_wcs(sdss_header)
                    sdss_proj, _ = reproject_interp(
                        (sdss_2d, sdss_wcs), common_wcs,
                        shape_out=shape_out,
                    )
            except Exception as e:
                self._log(f"SDSS reproject hata: {e}")
        lotss_proj = None
        try:
            lotss_proj, _ = reproject_interp(
                (lotss_data, lotss_wcs), common_wcs, shape_out=shape_out
            )
        except Exception as e:
            self._log(f"LoTSS reproject hata: {e}")
        return {
            "common_wcs": common_wcs,
            "shape": shape_out,
            "sdss": sdss_proj,
            "lotss": lotss_proj,
        }

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------
    def plot(self, ra: float, dec: float, size_arcmin: float = 3.0,
             mode: str = "both",
             save_path: Optional[str] = None,
             show: bool = True,
             rms: Optional[float] = None,
             figsize: Tuple[float, float] = (8, 8),
             title: Optional[str] = None,
             show_target_marker: bool = True,
             ) -> Optional[str]:
        """SDSS + LoTSS figuru olustur.

        Parameters
        ----------
        ra, dec : float
            Hedef koordinat (deg, ICRS).
        size_arcmin : float
            FOV (arcmin). LoTSS query bunu birebir kullanir.
        mode : str
            'both' (default), 'optical' veya 'radio'.
        save_path : str
            None ise figure_dir altinda otomatik isim.
        show : bool
            plt.show() cagrilsin mi.
        rms : float
            LoTSS RMS (Jy/beam). None ise sigma-clipped tahmin.
        figsize : (float, float)
            Matplotlib figure boyutu.
        title : str
            Ust baslik (None ise otomatik).
        show_target_marker : bool
            Cyan X markeri (default True).

        Returns
        -------
        str veya None
            Kaydedilen figur yolu; basarisiz ise None.
        """
        mode = str(mode).lower()
        if mode not in ("both", "optical", "radio"):
            raise ValueError(
                "mode must be 'both', 'optical', or 'radio'"
            )

        sdss_fits_path: Optional[str] = None
        lotss_data: Optional[np.ndarray] = None
        lotss_wcs: Optional[WCS] = None
        lotss_header = None

        if mode in ("both", "optical"):
            self._log(f"SDSS FITS fetch (band={self.sdss_band})")
            sdss_fits_path = self._fetch_sdss_fits(ra, dec, size_arcmin)
            if not sdss_fits_path and mode == "optical":
                self._log("SDSS FITS alinamadi; cizim atlandi.")
                return None

        if mode in ("both", "radio"):
            self._log(
                f"LoTSS fetch (release={self.lotss_release}, "
                f"lowres={self.lotss_lowres})"
            )
            try:
                lotss_data, lotss_wcs, lotss_header = self._fetch_lotss(
                    ra, dec, size_arcmin
                )
            except Exception as e:
                self._log(f"LoTSS fetch hata: {e}")
                if mode == "radio":
                    return None

        if rms is None and lotss_data is not None:
            rms = _estimate_rms(lotss_data)
            self._log(f"LoTSS RMS auto = {rms * 1e6:.2f} uJy/beam")

        aligned = None
        if (mode == "both" and lotss_data is not None
                and sdss_fits_path is not None):
            aligned = self._reproject_to_common(
                sdss_fits_path, lotss_data, lotss_wcs,
                ra, dec, size_arcmin,
            )

        fig = plt.figure(figsize=figsize)

        if mode == "both":
            return self._plot_both(
                fig, ra, dec, sdss_fits_path,
                lotss_data, lotss_wcs, aligned, rms,
                save_path, show, title, show_target_marker,
            )
        if mode == "optical":
            return self._plot_optical(
                fig, ra, dec, sdss_fits_path,
                save_path, show, title, show_target_marker,
            )
        return self._plot_radio(
            fig, ra, dec, lotss_data, lotss_wcs, rms,
            save_path, show, title, show_target_marker,
        )

    # -----------------------------------------------------------------
    def _plot_both(self, fig, ra, dec, sdss_fits_path,
                   lotss_data, lotss_wcs, aligned, rms,
                   save_path, show, title, show_target_marker):
        # 1) Ideal: her ikisi ortak north-up grid'e reproject edildi
        use_common = (
            aligned is not None
            and aligned.get("sdss") is not None
            and aligned.get("lotss") is not None
        )
        if use_common:
            proj_wcs = aligned["common_wcs"]
            sdss_arr = aligned["sdss"]
            radio_arr = aligned["lotss"]
        else:
            # Fallback: SDSS native WCS, LoTSS'i SDSS gridine reproject et
            sdss_arr = None
            radio_arr = None
            proj_wcs = None
            if sdss_fits_path and os.path.exists(sdss_fits_path):
                with fits.open(sdss_fits_path) as hdul:
                    primary = hdul[0]
                    if primary.data is None and len(hdul) > 1:
                        primary = hdul[1]
                    sdss_arr = _squeeze_2d(primary.data)
                    proj_wcs = _celestial_wcs(primary.header)
                if _HAVE_REPROJECT and lotss_data is not None:
                    try:
                        radio_arr, _ = reproject_interp(
                            (lotss_data, lotss_wcs), proj_wcs,
                            shape_out=sdss_arr.shape,
                        )
                    except Exception as e:
                        self._log(f"LoTSS->SDSS reproject hata: {e}")
            if proj_wcs is None and lotss_wcs is not None:
                proj_wcs = lotss_wcs
                radio_arr = lotss_data

        ax = fig.add_subplot(111, projection=proj_wcs)

        if sdss_arr is not None:
            ax.imshow(
                sdss_arr, origin="lower", cmap=self.cmap_optical,
                norm=_sdss_hard_contrast_norm(sdss_arr,
                                              self.sdss_n_sigma_dark),
            )

        if radio_arr is not None and rms is not None:
            levels = _contour_levels_log(rms, self.n_contour_levels)
            try:
                ax.contour(
                    radio_arr, levels=levels,
                    colors=self.contour_color,
                    linewidths=self.contour_linewidth,
                )
                ax.contour(
                    radio_arr, levels=[-3.0 * rms],
                    colors="gray", linewidths=0.6, linestyles="dashed",
                )
            except Exception as e:
                self._log(f"contour hata: {e}")

        if show_target_marker:
            try:
                ax.plot(
                    ra, dec, marker="x", color="cyan",
                    markersize=14, mew=2.5,
                    transform=ax.get_transform("world"), zorder=10,
                )
            except Exception:
                pass

        self._set_axes(ax)
        if title is None:
            title = (f"SDSS DR{self.sdss_dr} {self.sdss_band}-band + "
                     f"LoTSS {self.lotss_release.upper()}"
                     + (" (low-res)" if self.lotss_lowres else "")
                     + " contours")
        ax.set_title(title)

        return self._finalize(fig, ra, dec, save_path, show, "sdss_radio")

    # -----------------------------------------------------------------
    def _plot_optical(self, fig, ra, dec, sdss_fits_path,
                      save_path, show, title, show_target_marker):
        if not sdss_fits_path or not os.path.exists(sdss_fits_path):
            self._log("SDSS FITS yok; cizim atlandi.")
            plt.close(fig)
            return None
        with fits.open(sdss_fits_path) as hdul:
            primary = hdul[0]
            if primary.data is None and len(hdul) > 1:
                primary = hdul[1]
            sdss_arr = _squeeze_2d(primary.data)
            wcs = _celestial_wcs(primary.header)

        ax = fig.add_subplot(111, projection=wcs)
        ax.imshow(
            sdss_arr, origin="lower", cmap=self.cmap_optical,
            norm=_sdss_hard_contrast_norm(sdss_arr, self.sdss_n_sigma_dark),
        )
        if show_target_marker:
            try:
                ax.plot(
                    ra, dec, marker="x", color="cyan",
                    markersize=14, mew=2.5,
                    transform=ax.get_transform("world"), zorder=10,
                )
            except Exception:
                pass
        self._set_axes(ax)
        ax.set_title(title or f"SDSS DR{self.sdss_dr} {self.sdss_band}-band")
        return self._finalize(fig, ra, dec, save_path, show, "sdss_only")

    # -----------------------------------------------------------------
    def _plot_radio(self, fig, ra, dec, lotss_data, lotss_wcs, rms,
                    save_path, show, title, show_target_marker):
        if lotss_data is None or lotss_wcs is None:
            self._log("LoTSS verisi yok; cizim atlandi.")
            plt.close(fig)
            return None
        ax = fig.add_subplot(111, projection=lotss_wcs)
        ax.imshow(
            lotss_data, origin="lower", cmap=self.cmap_radio,
            norm=_radio_norm(lotss_data, "asinh"),
        )
        if rms is not None:
            levels = _contour_levels_log(rms, self.n_contour_levels)
            try:
                ax.contour(
                    lotss_data, levels=levels,
                    colors="white", linewidths=0.7, alpha=0.7,
                )
            except Exception:
                pass
        if show_target_marker:
            try:
                ax.plot(
                    ra, dec, marker="x", color="cyan",
                    markersize=14, mew=2.5,
                    transform=ax.get_transform("world"), zorder=10,
                )
            except Exception:
                pass
        self._set_axes(ax)
        ax.set_title(title or (
            f"LoTSS {self.lotss_release.upper()}"
            + (" (low-res)" if self.lotss_lowres else "")
        ))
        return self._finalize(fig, ra, dec, save_path, show, "lotss_only")

    # -----------------------------------------------------------------
    def _set_axes(self, ax):
        try:
            ax.coords[0].set_ticks_position('b')
            ax.coords[0].set_ticklabel_position('b')
            ax.coords[0].set_axislabel_position('b')
            ax.coords[0].set_axislabel('RA (J2000)')
            ax.coords[1].set_ticks_position('l')
            ax.coords[1].set_ticklabel_position('l')
            ax.coords[1].set_axislabel_position('l')
            ax.coords[1].set_axislabel('Dec (J2000)')
            ax.coords.grid(color="0.7", ls=":", alpha=0.4)
        except Exception:
            pass

    # -----------------------------------------------------------------
    def _finalize(self, fig, ra, dec, save_path, show, prefix):
        if not save_path:
            save_path = os.path.join(
                self.figure_dir,
                f"{prefix}_ra{ra:.4f}_dec{dec:+.4f}.png",
            )
        try:
            fig.savefig(save_path, dpi=120, bbox_inches="tight")
            self._log(f"figure saved -> {save_path}")
        except Exception as e:
            self._log(f"figure save hata: {e}")
            save_path = None
        if show:
            plt.show()
        else:
            plt.close(fig)
        return save_path


# ====================================================================
# Modul-seviyesi kisa-yol
# ====================================================================
def sdss_radio(ra: float, dec: float, size_arcmin: float = 3.0,
               mode: str = "both", **kwargs) -> Optional[str]:
    """Tek seferlik cizim icin kisa-yol.

    SDSSRadioPlotter constructor parametreleri ve plot() parametreleri
    **kwargs ile gecirilebilir. Ornek:

        sdss_radio(ra=187.27, dec=2.05, size_arcmin=3.0,
                   lotss_release='dr3', mode='both', show=True)
    """
    plot_keys = {"save_path", "show", "rms", "figsize", "title",
                 "show_target_marker"}
    plot_kwargs = {k: kwargs.pop(k) for k in list(kwargs) if k in plot_keys}
    plotter = SDSSRadioPlotter(**kwargs)
    return plotter.plot(
        ra=ra, dec=dec, size_arcmin=size_arcmin,
        mode=mode, **plot_kwargs,
    )


__all__ = [
    "SDSSRadioPlotter",
    "sdss_radio",
    "fetch_lotss_cutout",
]
