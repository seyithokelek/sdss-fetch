import os
import requests
import pandas as pd
from PIL import Image
from io import BytesIO
import time
from .utils import log_message, handle_exception, validate_coordinates, get_sdss_config
import matplotlib.pyplot as plt
from astroquery.sdss import SDSS
from astropy.coordinates import SkyCoord
from astropy.io import fits
import astropy.units as u

class CutoutFetcher:
    """
    CutoutFetcher
    -------------
    Downloads SDSS SkyServer JPEG cutout images and FITS science images.

    Parameters:
    - output_dir (str): Directory to save JPEG cutouts (default: 'cutouts')
    - scale (float): Arcseconds per pixel (default: 0.2)
    - size (int): Image width/height in pixels (default: 512)
    - opt (str): Overlay options: 'S', 'L', 'G', or combinations like 'SLG'
    - max_retries (int): Retry attempts on failure (default: 2)
    - retry_wait (int): Wait time between retries (seconds, default: 15)
    - data_release (int): SDSS data release to use (default: 16)

    Methods:
    - set_coordinates(ra, dec): Set current working coordinates
    - fetch_single(): Download JPEG cutout for current or given coordinates
    - fetch_fits_image(): Download FITS science image for current or given coordinates
    - fetch_all(df): Download JPEG cutouts for DataFrame of coordinates
    """

    def __init__(self, output_dir: str = "cutouts", scale: float = 0.2, size: int = 512,
                 opt: str = "SLG", max_retries: int = 2, retry_wait: int = 15, data_release: int = 16):
        self.output_dir = output_dir
        self.scale = scale
        self.size = size
        self.opt = opt
        self.max_retries = max_retries
        self.retry_wait = retry_wait
        self.ra = None
        self.dec = None
        self.config = get_sdss_config(data_release)

        os.makedirs(self.output_dir, exist_ok=True)

    def set_coordinates(self, ra: float, dec: float):
        if validate_coordinates(ra, dec):
            self.ra = ra
            self.dec = dec
            log_message(f"Coordinates set to RA={ra}, DEC={dec}")
            return True
        return False

    def _construct_url(self, ra: float, dec: float) -> str:
        base_url = (
            f"{self.config['skyserver_base']}/SkyServerWS/ImgCutout/getjpeg"
            f"?TaskName=Skyserver.Chart.Image"
            f"&ra={ra}&dec={dec}"
            f"&scale={self.scale}&width={self.size}&height={self.size}"
        )

        if not self.opt.strip():
            return base_url

        overlay_params = []
        if "S" in self.opt.upper():
            overlay_params.append("SpecObjs=on")
        if "L" in self.opt.upper():
            overlay_params.append("Label=on")
        if "G" in self.opt.upper():
            overlay_params.append("Grid=on")

        return base_url + f"&opt={self.opt}" + "&query=&" + "&".join(overlay_params)

    def _generate_filename(self, ra: float, dec: float, prefix: str, ext: str) -> str:
        base = f"{prefix}-ra{ra:.4f}-dec{dec:.4f}"
        if self.scale != 0.2:
            base += f"_s{self.scale}"
        if self.size != 512:
            base += f"_{self.size}"
        return base + f".{ext}"

    def _resolve_filepath(self, filename: str, folder: str) -> str:
        filepath = os.path.join(folder, filename)
        counter = 2
        while os.path.exists(filepath):
            name, ext = os.path.splitext(filename)
            filepath = os.path.join(folder, f"{name}_{counter}{ext}")
            counter += 1
        return filepath

    def fetch_single(self, ra: float = None, dec: float = None, filename: str = None) -> str:
        ra = ra if ra is not None else self.ra
        dec = dec if dec is not None else self.dec

        if ra is None or dec is None:
            log_message("RA/DEC not provided or set. Use set_coordinates() first.")
            return ""

        if not validate_coordinates(ra, dec):
            return ""

        url = self._construct_url(ra, dec)
        for attempt in range(1, self.max_retries + 2):
            try:
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()
                image = Image.open(BytesIO(response.content))

                if not filename:
                    filename = self._generate_filename(ra, dec, "image", "png")
                filepath = self._resolve_filepath(filename, self.output_dir)

                image.save(filepath)

                log_message(f"Saved cutout: {filepath}")
                log_message(f"→ Download URL: {url}")
                log_message(f"→ RA={ra}, DEC={dec}, scale={self.scale}, size={self.size}, opt='{self.opt}'")

                plt.imshow(image)
                plt.title(os.path.basename(filepath))
                plt.axis('off')
                plt.show()

                return filepath
            except Exception as e:
                log_message(f"Cutout failed for RA={ra}, DEC={dec} on attempt {attempt}: {e}")
                if attempt < self.max_retries + 1:
                    log_message(f"Retrying in {self.retry_wait} seconds...")
                    time.sleep(self.retry_wait)
        
        handle_exception("fetch_single", Exception(f"All {self.max_retries} attempts failed"))
        return ""

    def fetch_fits_image(self, band: str = "g", ra: float = None, dec: float = None, filename: str = None) -> str:
        ra = ra if ra is not None else self.ra
        dec = dec if dec is not None else self.dec

        if ra is None or dec is None:
            log_message("RA/DEC not provided or set. Use set_coordinates() first.")
            return ""

        if not validate_coordinates(ra, dec):
            return ""

        try:
            coord = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, frame="icrs")
            images = SDSS.get_images(coordinates=coord, band=band, radius=0.02 * u.deg, 
                                     data_release=self.config["data_release"])
            if not images:
                log_message(f"No FITS image found for RA={ra}, DEC={dec}, band={band}")
                return ""
            hdu = images[0]
            os.makedirs("fits_images", exist_ok=True)
            if not filename:
                filename = self._generate_filename(ra, dec, f"fits-band{band}", "fits")
            filepath = self._resolve_filepath(filename, "fits_images")
            hdu.writeto(filepath, overwrite=True)
            log_message(f"Saved FITS image: {filepath}")
            return filepath
        except Exception as e:
            handle_exception("fetch_fits_image", e)
            return ""

    def fetch_all(self, df: pd.DataFrame, ra_col: str = "ra", dec_col: str = "dec"):
        results = []
        for _, row in df.iterrows():
            if self.set_coordinates(row[ra_col], row[dec_col]):
                filepath = self.fetch_single()
                if filepath:
                    results.append(filepath)
        
        log_message(f"Fetched {len(results)} of {len(df)} cutouts successfully")
        return results
