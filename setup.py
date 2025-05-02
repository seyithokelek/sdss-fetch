from setuptools import setup, find_packages
import os

# Read the contents of README.md file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="sdss_fetch",
    version="0.2.0",
    description="A modular SDSS spectra and cutout downloader",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Seyit Hökelek",
    author_email="sthokelek@gmail.com",
    url="https://github.com/seyithokelek/sdss-fetch",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.0",
        "pandas>=1.2.0",
        "astropy>=4.2",
        "astroquery>=0.4.1",
        "pillow>=8.0.0",
        "matplotlib>=3.3.0",
        "numpy>=1.19.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Astronomy",
    ],
    keywords="astronomy, sdss, spectra, astrophysics, data-analysis",
    python_requires='>=3.7',
    include_package_data=True,
    zip_safe=False,
    project_urls={
        "Bug Tracker": "https://github.com/seyithokelek/sdss-fetch/issues",
        "Documentation": "https://github.com/seyithokelek/sdss-fetch",
        "Source Code": "https://github.com/seyithokelek/sdss-fetch",
    }
)
