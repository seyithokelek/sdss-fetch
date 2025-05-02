# SDSS Fetch

A modular Python package for downloading and organizing SDSS spectroscopic and image data efficiently. At least it will be so.

## Features
- Multi-threaded SDSS spectrum downloader (DR16-prioritized)
- Fallback from SAS → API if needed
- Customizable DR and survey config
- Image cutouts by RA/DEC coordinates
- Spectrum preview thumbnails
- Metadata extraction (z, S/N, emission lines)
- Astroquery-based RA/DEC → plate-mjd-fiber resolver
- Photometry data retrieval (ugriz magnitudes)
- Caching mechanism for improved performance
- Standardized error handling

## Installation

```bash
pip install -e .
```

Or directly from GitHub:

```bash
git clone https://github.com/seyithokelek/sdss-fetch.git
cd sdss-fetch
pip install -e .
```

## Modules

- **SDSSParallelDownloader**: Download SDSS spectra in parallel with fallback mechanisms
- **CutoutFetcher**: Get JPEG and FITS image cutouts from SDSS SkyServer
- **SpectrumPreview**: Generate spectrum preview images by specobj ID
- **TargetResolver**: Resolve coordinates to SDSS spectroscopic objects
- **MetadataExtractor**: Extract metadata and emission lines from FITS files
- **PhotometryFetcher**: Query SDSS ugriz photometric magnitudes

## Examples

Check the `examples/` directory for more detailed usage examples:
- Bulk downloading spectra
- Working with cutouts and previews
- Extracting metadata from FITS files
- Resolving coordinates to SDSS objects
- Querying photometry data

## License

MIT
