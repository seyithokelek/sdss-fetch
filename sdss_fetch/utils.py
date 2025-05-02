import os
from datetime import datetime
import pandas as pd
from astropy.table import Table

def log_message(message: str, logfile: str = "sdss_fetch.log", print_console: bool = True):
    """Log a timestamped message to a logfile and optionally print to console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logline = f"[{timestamp}] {message}\n"
    os.makedirs(os.path.dirname(logfile) or ".", exist_ok=True)
    with open(logfile, "a") as f:
        f.write(logline)
    if print_console:
        print(logline.strip())

def safe_to_csv(df: pd.DataFrame, filename: str, convert_columns=("specobjid", "objid", "fiberID")):
    """Safely write DataFrame to CSV with large IDs as string."""
    df = df.copy()
    for col in convert_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    ensure_dir(filename)
    df.to_csv(filename, index=False)
    log_message(f"Saved DataFrame to CSV: {filename}")

def safe_to_fits(df: pd.DataFrame, filename: str, convert_columns=("specobjid", "objid", "fiberID")):
    """Safely write DataFrame to FITS with large IDs as string."""
    df = df.copy()
    for col in convert_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    ensure_dir(filename)
    table = Table.from_pandas(df)
    table.write(filename, overwrite=True)
    log_message(f"Saved DataFrame to FITS: {filename}")

def ensure_dir(path: str):
    """Ensure the directory for the given path exists."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

def get_unique_filename(filepath: str) -> str:
    """Generate a unique filename by appending a counter if file exists."""
    base, ext = os.path.splitext(filepath)
    counter = 2
    new_path = filepath
    while os.path.exists(new_path):
        new_path = f"{base}_{counter}{ext}"
        counter += 1
    return new_path

def validate_dataframe(df: pd.DataFrame, required_cols: list) -> bool:
    """Check for required columns in a DataFrame."""
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        log_message(f"Missing required columns: {missing}")
        return False
    return True

def handle_exception(func_name: str, error: Exception, logfile: str = "sdss_fetch.log", print_console: bool = True):
    """Standardized exception handling and logging."""
    message = f"Error in {func_name}: {str(error)}"
    log_message(message, logfile, print_console)
    return None

def get_sdss_config(data_release: int = 16):
    """Get configuration for a specific SDSS data release."""
    config = {
        "skyserver_base": f"https://skyserver.sdss.org/dr{data_release}",
        "sas_base": f"https://dr{data_release}.sdss.org/sas",
        "api_base": f"https://dr{data_release}.sdss.org/optical/spectrum/view",
        "data_release": data_release
    }
    return config

def validate_coordinates(ra: float, dec: float) -> bool:
    """Validate if RA/DEC coordinates are within valid ranges."""
    if not (0 <= ra < 360):
        log_message(f"Invalid RA value: {ra}. Must be between 0 and 360.")
        return False
    
    if not (-90 <= dec <= 90):
        log_message(f"Invalid DEC value: {dec}. Must be between -90 and 90.")
        return False
    
    return True
