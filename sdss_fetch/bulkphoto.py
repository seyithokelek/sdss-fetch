import pandas as pd
import numpy as np
from tqdm import tqdm
from astroquery.sdss import SDSS
from astropy.coordinates import SkyCoord
import astropy.units as u
from datetime import datetime
import os
SDSS.TIMEOUT = 180 

class RegionBasedPhotometryFetcher:
    def __init__(self, data_release: int = 16):
        self.data_release = data_release
        self.empty_regions = []
        self.failed_regions = []
        self.unmatched_objects = []

        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = os.path.join(log_dir, f"sdss_log_{now}.txt")

    def log(self, message: str):
        with open(self.log_file, "a") as f:
            f.write(f"[{datetime.now()}] {message}\n")
        print(message)

    def assign_regions(self, df: pd.DataFrame, ra_col="RA", dec_col="DEC", region_size_deg: float = 1.5) -> pd.DataFrame:
        df = df.copy()
        df["ra_bin"] = (df[ra_col] // region_size_deg).astype(int)
        df["dec_bin"] = (df[dec_col] // region_size_deg).astype(int)
        df["region_id"] = df["ra_bin"].astype(str) + "_" + df["dec_bin"].astype(str)
        return df

    def query_sdss_per_region(self, df: pd.DataFrame,
                              region_size_deg: float = 1.5,
                              checkpoint_path: str = None,
                              checkpoint_step: int = 1000) -> pd.DataFrame:

        df = self.assign_regions(df, region_size_deg=region_size_deg)
        unique_regions = df["region_id"].unique()
        all_results = []
        chunk_results = []

        for i, region in enumerate(tqdm(unique_regions, desc="Querying per region")):
            ra_bin, dec_bin = map(int, region.split("_"))
            ra_center = (ra_bin + 0.5) * region_size_deg
            dec_center = (dec_bin + 0.5) * region_size_deg
            center_coord = SkyCoord(ra=ra_center * u.deg, dec=dec_center * u.deg)
            width = height = region_size_deg * u.deg

            try:
                result = SDSS.query_region(
                    center_coord,
                    width=width,
                    height=height,
                    photoobj_fields=[
                        "ra", "dec", "objid", "u", "g", "r", "i", "z",
                        "extinction_u", "extinction_g", "extinction_r", "extinction_i", "extinction_z",
                        "psfFlux_u", "psfFlux_g", "psfFlux_r", "psfFlux_i", "psfFlux_z"
                    ],
                    data_release=self.data_release
                )

                if result is not None and len(result) > 0:
                    df_result = result.to_pandas()
                    df_result["region_id"] = region
                    chunk_results.append(df_result)
                else:
                    self.empty_regions.append(region)
                    self.log(f"[EMPTY] No data returned for region {region}")

            except Exception as e:
                self.failed_regions.append(region)
                self.log(f"[ERROR] Failed region {region} → {str(e)}")
                continue

            if checkpoint_path and (i + 1) % checkpoint_step == 0 and chunk_results:
                partial_df = pd.concat(chunk_results, ignore_index=True)
                if os.path.exists(checkpoint_path):
                    old_df = pd.read_parquet(checkpoint_path)
                    combined = pd.concat([old_df, partial_df], ignore_index=True)
                else:
                    combined = partial_df

                combined.to_parquet(checkpoint_path, engine='pyarrow', compression='snappy', index=False)

                self.log(f"[Checkpoint] {i + 1} regions written to {checkpoint_path}")
                all_results.extend(chunk_results)
                chunk_results.clear()

        if checkpoint_path and chunk_results:
            partial_df = pd.concat(chunk_results, ignore_index=True)
            partial_df.to_parquet(checkpoint_path, engine='pyarrow', compression='snappy', index=False, append=True)
            self.log(f"[Checkpoint] Final chunk written.")
            all_results.extend(chunk_results)

        return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

    def match_to_input(self, df_input: pd.DataFrame, df_sdss: pd.DataFrame, radius_arcsec: float = 3.0) -> pd.DataFrame:
        df_sdss.columns = df_sdss.columns.str.upper()
        if not {"RA", "DEC"}.issubset(df_sdss.columns):
            raise ValueError("RA/DEC columns not found in SDSS result.")

        # SkyCoord nesneleri
        coords_input = SkyCoord(ra=df_input["RA"].values * u.deg, dec=df_input["DEC"].values * u.deg)
        coords_sdss = SkyCoord(ra=df_sdss["RA"].values * u.deg, dec=df_sdss["DEC"].values * u.deg)

        # En yakın komşu eşleştirme
        idx, d2d, _ = coords_input.match_to_catalog_sky(coords_sdss)
        matched = df_sdss.iloc[idx].reset_index(drop=True)

        # RA/DEC farklarını ve separation'ı hesapla
        ra_input = df_input["RA"].values
        dec_input = df_input["DEC"].values
        ra_matched = matched["RA"].values
        dec_matched = matched["DEC"].values

        ra_diff_arcsec = (ra_input - ra_matched) * 3600.0 * np.cos(np.deg2rad(dec_input))
        dec_diff_arcsec = (dec_input - dec_matched) * 3600.0
        separation_arcsec = d2d.arcsecond

        # Kalite etiketi
        match_quality = np.where(separation_arcsec > 2.0, "bad", "good")

        # Eşleşen veriyi organize et
        matched_clean = pd.DataFrame({
            "input_index": df_input.index,
            "RA_input": ra_input,
            "DEC_input": dec_input,
            "RA_matched": ra_matched,
            "DEC_matched": dec_matched,
            "delta_ra_arcsec": ra_diff_arcsec,
            "delta_dec_arcsec": dec_diff_arcsec,
            "separation_arcsec": separation_arcsec,
            "match_quality": match_quality
        })

        # Eşik içinde olanlar
        within_radius = separation_arcsec <= radius_arcsec
        unmatched_input = df_input[~within_radius]

        if not unmatched_input.empty:
            self.unmatched_objects = unmatched_input.copy()
            self.log(f"[MATCH] {len(unmatched_input)} objects could not be matched within {radius_arcsec} arcsec")

        # Sonuçları birleştir
        df_input_reset = df_input.reset_index(drop=True)
        df_matched_final = pd.concat([
            df_input_reset[within_radius].reset_index(drop=True),
            matched_clean[within_radius].reset_index(drop=True),
            matched[within_radius].reset_index(drop=True).drop(columns=["RA", "DEC"])
        ], axis=1)

        return df_matched_final



    def save_logs_and_unmatched(self, base_name="sdss_process"):
        if self.unmatched_objects:
            pd.DataFrame(self.unmatched_objects).to_csv(f"{base_name}_unmatched.csv", index=False)
        if self.empty_regions:
            pd.DataFrame({"region_id": self.empty_regions}).to_csv(f"{base_name}_empty_regions.csv", index=False)
        if self.failed_regions:
            pd.DataFrame({"region_id": self.failed_regions}).to_csv(f"{base_name}_failed_regions.csv", index=False)
        self.log("✔ Log and unmatched/failed regions saved.")
