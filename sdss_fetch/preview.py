import os
import requests
from PIL import Image
from io import BytesIO
import pandas as pd
import time
import matplotlib.pyplot as plt
from .utils import log_message, handle_exception, get_sdss_config

class SpectrumPreview:
    """
    SpectrumPreview
    ----------------
    Downloads spectrum preview images from SDSS SkyServer using the object's specobjid.

    Parameters:
    - output_dir (str): Directory where images will be saved (default: 'previews')
    - max_retries (int): Number of retry attempts in case of failure (default: 2)
    - retry_wait (int): Seconds to wait between retries (default: 15)
    - data_release (int): SDSS data release to use (default: 16)

    Methods:
    - fetch_single(specobjid): Download and display preview for a single specobjid
    - fetch_all(df): Batch download previews from a DataFrame with a 'specobjid' column
    - fetch_from_resolved(resolved_df): Directly use resolved DataFrame for previews
    """

    def __init__(self, output_dir: str = "previews", max_retries: int = 2, 
                 retry_wait: int = 15, data_release: int = 16):
        self.output_dir = output_dir
        self.max_retries = max_retries
        self.retry_wait = retry_wait
        self.config = get_sdss_config(data_release)
        os.makedirs(self.output_dir, exist_ok=True)

    def _construct_url(self, specobjid: int) -> str:
        """
        Construct URL for spectrum preview image.
        
        Parameters:
        - specobjid: SDSS spectroscopic object ID
        
        Returns:
        - URL string
        """
        return f"{self.config['skyserver_base']}/en/get/SpecById.ashx?id={int(specobjid)}"

    def _resolve_filepath(self, specobjid: int) -> str:
        """
        Generate a unique filename for the preview image.
        
        Parameters:
        - specobjid: SDSS spectroscopic object ID
        
        Returns:
        - Filepath string
        """
        base_filename = f"spec-{int(specobjid)}.png"
        filepath = os.path.join(self.output_dir, base_filename)
        counter = 2
        while os.path.exists(filepath):
            name, ext = os.path.splitext(base_filename)
            filepath = os.path.join(self.output_dir, f"{name}_{counter}{ext}")
            counter += 1
        return filepath

    def fetch_single(self, specobjid: int, filename: str = None, display: bool = True) -> str:
        """
        Download and optionally display a spectrum preview for a single specobjid.
        
        Parameters:
        - specobjid: SDSS spectroscopic object ID
        - filename: Optional custom filename
        - display: Whether to display the image (default: True)
        
        Returns:
        - Path to saved image or empty string on failure
        """
        url = self._construct_url(specobjid)
        for attempt in range(1, self.max_retries + 2):
            try:
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()
                image = Image.open(BytesIO(response.content))

                if not filename:
                    filepath = self._resolve_filepath(specobjid)
                else:
                    filepath = os.path.join(self.output_dir, filename)

                image.save(filepath)
                log_message(f"Saved preview: {filepath}")

                if display:
                    plt.figure(figsize=(10, 6))
                    plt.imshow(image)
                    plt.title(f"Spectrum: {specobjid}")
                    plt.axis('off')
                    plt.show()

                return filepath
            except Exception as e:
                log_message(f"Preview failed for specobjid={specobjid} on attempt {attempt}: {e}")
                if attempt < self.max_retries + 1:
                    log_message(f"Retrying in {self.retry_wait} seconds...")
                    time.sleep(self.retry_wait)
        
        handle_exception("fetch_single", Exception(f"Failed to fetch preview for specobjid={specobjid}"))
        return ""

    def fetch_all(self, df: pd.DataFrame, id_col: str = "specobjid", display: bool = False) -> list:
        """
        Fetch previews for all specobjids in a DataFrame.
        
        Parameters:
        - df: DataFrame containing specobjids
        - id_col: Name of column containing specobjids (default: "specobjid")
        - display: Whether to display each image (default: False)
        
        Returns:
        - List of paths to successfully saved images
        """
        if id_col not in df.columns:
            log_message(f"Column '{id_col}' not found in DataFrame")
            return []
            
        results = []
        for idx, row in df.iterrows():
            specobjid = row[id_col]
            log_message(f"Fetching preview for row {idx}, {id_col}={specobjid}")
            filepath = self.fetch_single(specobjid, display=display)
            if filepath:
                results.append(filepath)
                
        log_message(f"Fetched {len(results)} of {len(df)} previews successfully")
        return results

    def fetch_from_resolved(self, resolved_df: pd.DataFrame, display: bool = False) -> list:
        """
        Accepts output of resolver.resolve_all and fetches previews for all.

        Parameters:
        - resolved_df (DataFrame): Must contain 'specobjid' column
        - display: Whether to display each image (default: False)
        
        Returns:
        - List of paths to successfully saved images
        """
        if "specobjid" not in resolved_df.columns:
            log_message("No 'specobjid' column found in input DataFrame for preview generation.")
            return []
            
        return self.fetch_all(resolved_df, id_col="specobjid", display=display)
