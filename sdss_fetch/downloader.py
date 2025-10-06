import os
import time
import requests
import argparse
from typing import List, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

class SDSSParallelDownloader:
    def __init__(self, output_dir: str = "spectra", max_workers: int = 8, user_agent: str = None):
        self.output_dir = output_dir
        self.max_workers = max_workers
        os.makedirs(self.output_dir, exist_ok=True)

        self.dr_config = {
            "dr16": ("v5_13_0", "eboss", "dr16"),
            "dr17": ("v5_13_2", "eboss", "dr17"),
            "dr18": ("v6_0_4", "bhm", "dr18"),
            "dr15": ("v5_10_0", "eboss", "dr15"),
            "dr14": ("v5_10_0", "eboss", "dr14"),
            "dr13": ("v5_9_0", "eboss", "dr13"),
            "dr12": ("v5_7_0", "boss", "dr12"),
            "dr11": ("v5_7_0", "boss", "dr11"),
            "dr10": ("v5_5_12", "boss", "dr10"),
            "dr9": ("v5_5_12", "boss", "dr9"),
            "dr8": ("v5_4_45", "sdss", "dr8")
        }

        self.run2d_multi = {
            "dr16": ["26", "103", "104", "v5_13_0"],
            "dr17": ["v5_13_2"],
            "dr18": ["v6_0_4"],
            "dr15": ["v5_10_0"],
            "dr14": ["v5_10_0", "26", "103", "104"],
            "dr13": ["v5_9_0"],
            "dr12": ["v5_7_0", "v5_7_2", "26", "103", "104"],
            "dr11": ["v5_7_0"],
            "dr10": ["v5_5_12", "26", "103", "104"],
            "dr9":  ["v5_5_12"],
            "dr8":  ["v5_4_45"]
        }

        self.dr_list = list(self.dr_config.keys())

        default_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
        self.headers = {
            "Connection": "close",
            "User-Agent": user_agent if user_agent else default_agent
        }

    def construct_sas_url(self, dr: str, plate: int, mjd: int, fiber: int) -> str:
        run2d, survey, dr_dir = self.dr_config[dr]
        return f"https://{dr}.sdss.org/sas/{dr}/spectro/{survey}/redux/{run2d}/spectra/lite/{plate:04d}/spec-{plate:04d}-{mjd}-{fiber:04d}.fits"

    def construct_api_url(self, dr: str, plate: int, mjd: int, fiber: int) -> str:
        return f"https://{dr}.sdss.org/optical/spectrum/view/data/format=fits/spec=lite?plateid={plate}&mjd={mjd}&fiberid={fiber}"

    def construct_sdss_org_url(self, dr: str, run2d: str, plate: int, mjd: int, fiber: int) -> str:
        return f"https://data.sdss.org/sas/{dr}/sdss/spectro/redux/{run2d}/spectra/lite/{plate:04d}/spec-{plate:04d}-{mjd}-{fiber:04d}.fits"

    def get_unique_filename(self, filepath: str) -> str:
        base, ext = os.path.splitext(filepath)
        counter = 2
        new_path = filepath
        while os.path.exists(new_path):
            new_path = f"{base}_{counter}{ext}"
            counter += 1
        return new_path

    def log_message(self, message: str, logfile: str = "downloader.log"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(logfile, "a") as f:
            f.write(f"[{timestamp}] {message}\n")

    def log_failed(self, plate: int, mjd: int, fiber: int, failfile: str = "failed_list.txt"):
        line = f"{plate}\t{mjd}\t{fiber}\n"
        with open(failfile, "a") as f:
            f.write(line)

    def download_with_retry(self, url: str, filepath: str, verbose: bool, index: int) -> Tuple[bool, str]:
        for attempt in range(2):
            try:
                r = requests.get(url, stream=True, timeout=10, headers=self.headers)
                if r.status_code == 200:
                    final_path = self.get_unique_filename(filepath)
                    with open(final_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                    self.log_message(f"✓ Downloaded: {final_path} ← {url}")
                    if verbose:
                        print(f"[{index+1}] ✓ {os.path.basename(final_path)}")
                    return True, os.path.basename(final_path)
            except Exception as e:
                self.log_message(f"× Attempt {attempt+1} failed: {url} → {e}")
            time.sleep(5)
        self.log_message(f"× Final failure: {url}")
        return False, ""

    def try_download(self, plate: int, mjd: int, fiber: int, index: int, verbose: bool) -> Tuple[str, bool]:
            if verbose:
                print(f"[{index+1}] Starting download...")

            filename_base = f"spec-{plate:04d}-{mjd}-{fiber:05d}.fits"
            filepath = os.path.join(self.output_dir, filename_base)

            # Firstly: Fast and Safe (Priority DR16 Lite SAS Path)
            plate_str = f"{plate:04d}"
            mjd_str = str(mjd)
            fiber_str = f"{fiber:04d}"
            
            priority_url = f"https://dr16.sdss.org/sas/dr16/eboss/spectro/redux/v5_13_0/spectra/lite/{plate_str}/spec-{plate_str}-{mjd_str}-{fiber_str}.fits"

            success, name = self.download_with_retry(priority_url, filepath, verbose, index)
            if success:
                self.log_message(f"✓ {name} (Priority DR16 Lite SAS)")
                return name, True
            
            if verbose:
                print(f"[{index+1}] Priority DR16 Lite SAS failed. Trying fallback methods...")

            # Fallback Method 1: sdss.org/sas path (using run2d_multi list)
            for dr in self.dr_list:
                for run2d in self.run2d_multi.get(dr, []):
                    url = self.construct_sdss_org_url(dr, run2d, plate, mjd, fiber)
                    success, name = self.download_with_retry(url, filepath, verbose, index)
                    if success:
                        self.log_message(f"✓ {name} ({dr} sdss.org run2d={run2d})")
                        return name, True
            
            # Fallback Method 2: DR16 API
            url = self.construct_api_url("dr16", plate, mjd, fiber)
            success, name = self.download_with_retry(url, filepath, verbose, index)
            if success:
                self.log_message(f"✓ {name} (DR16 API)")
                return name, True

            # Fallback Method 3: General SAS Path (dr_config) + API
            for dr in self.dr_list:
                # SAS Path 
                sas_url = self.construct_sas_url(dr, plate, mjd, fiber)
                success, name = self.download_with_retry(sas_url, filepath, verbose, index)
                if success:
                    self.log_message(f"✓ {name} ({dr} SAS)")
                    return name, True

                # API Path 
                api_url = self.construct_api_url(dr, plate, mjd, fiber)
                success, name = self.download_with_retry(api_url, filepath, verbose, index)
                if success:
                    self.log_message(f"✓ {name} ({dr} API)")
                    return name, True

            # Final Failure
            self.log_message(f"× {filename_base}: All methods failed.")
            self.log_failed(plate, mjd, fiber)
            if verbose:
                print(f"[{index+1}] × {filename_base}: All methods failed.")
            return filename_base, False

    def download_all(self, plates: List[int], mjds: List[int], fibers: List[int], verbose: bool = True):
        start = datetime.now()
        if not verbose:
            print(f"Started at {start.strftime('%Y-%m-%d %H:%M:%S')}")

        args = list(enumerate(zip(plates, mjds, fibers)))
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            list(executor.map(lambda p: self.try_download(*p[1], index=p[0], verbose=verbose), args))

        end = datetime.now()
        if not verbose:
            print(f"Completed at {end.strftime('%Y-%m-%d %H:%M:%S')}")
            duration = (end - start).total_seconds() / 60
            print(f"Duration: {duration:.2f} minutes")
