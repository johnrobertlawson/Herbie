## Added by Seth Lyman
## April 2025

"""
A Herbie template for the NOAA Air Quality Model (AQM).

The National Air Quality Forecasting Capability (NAQFC) dataset contains model-generated
Air-Quality (AQ) forecast guidance from the Air Quality Model (AQM). It produces forecast guidance
for ozone (O3) and particulate matter with diameter equal to or less than 2.5 micrometers (PM2.5)
using meteorological forecasts based on NCEP's operational weather forecast models.

Available domains:
- "CS" - CONUS (Continental United States) - ~5km resolution
- "AK" - Alaska - ~6km resolution
- "HI" - Hawaii - ~2.5km resolution

Available products:
- "ave_1hr_o3" - 1-hour average ozone
- "ave_1hr_o3_bc" - 1-hour average ozone (bias-corrected)
- "ave_8hr_o3" - 8-hour average ozone
- "ave_8hr_o3_bc" - 8-hour average ozone (bias-corrected)
- "max_1hr_o3" - Maximum 1-hour ozone
- "max_1hr_o3_bc" - Maximum 1-hour ozone (bias-corrected)
- "max_8hr_o3" - Maximum 8-hour ozone
- "max_8hr_o3_bc" - Maximum 8-hour ozone (bias-corrected)
- "ave_1hr_pm25" - 1-hour average PM2.5
- "ave_1hr_pm25_bc" - 1-hour average PM2.5 (bias-corrected)
- "ave_24hr_pm25" - 24-hour average PM2.5
- "ave_24hr_pm25_bc" - 24-hour average PM2.5 (bias-corrected)
- "max_1hr_pm25" - Maximum 1-hour PM2.5
- "max_1hr_pm25_bc" - Maximum 1-hour PM2.5 (bias-corrected)

The AQM produces hourly forecast guidance for O3 and PM2.5 out to 72 hours twice per day,
starting at 0600 and 1200 UTC.

Note: Unlike other NOAA models, AQM files don't encode the forecast hour in the filename.
Instead, they use a constant "227" identifier in the filename regardless of the forecast hour.
The forecast hour is encoded within the GRIB file itself.

References:
- https://registry.opendata.aws/noaa-nws-naqfc-pds/
- https://vlab.noaa.gov/web/osti-modeling/air-quality
"""

import warnings
import re
from datetime import datetime

import xarray as xr

class aqm:
    def template(self):
        self.model = "aqm"
        self.DESCRIPTION = "NOAA Air Quality Model (AQM)"
        self.DETAILS = {
            "Product description": "https://registry.opendata.aws/noaa-nws-naqfc-pds/",
            "Model documentation": "https://vlab.noaa.gov/web/osti-modeling/air-quality",
        }

        # Set default domain if not provided
        if not hasattr(self, "domain"):
            self.domain = "CS"  # Default to CONUS domain

        # Set the model version
        if not hasattr(self, "version"):
            # Default to current operational version (v7 as of April 2025)
            self.version = "v7"

        # Starting May 14, 2024, AQMv7 became operational
        # July 20, 2021 to May 13, 2024 was AQMv6
        # Before July 20, 2021 was AQMv5
        if self.date.year < 2021 or (self.date.year == 2021 and self.date.month < 7):
            self.version = "v5"
        elif self.date.year < 2024 or (self.date.year == 2024 and self.date.month < 5) or (self.date.year == 2024 and self.date.month == 5 and self.date.day < 14):
            self.version = "v6"

        self.PRODUCTS = {
            "ave_1hr_o3": "1-hour average ozone",
            "ave_1hr_o3_bc": "1-hour average ozone (bias-corrected)",
            "ave_8hr_o3": "8-hour average ozone",
            "ave_8hr_o3_bc": "8-hour average ozone (bias-corrected)",
            "max_1hr_o3": "Maximum 1-hour ozone",
            "max_1hr_o3_bc": "Maximum 1-hour ozone (bias-corrected)",
            "max_8hr_o3": "Maximum 8-hour ozone",
            "max_8hr_o3_bc": "Maximum 8-hour ozone (bias-corrected)",
            "ave_1hr_pm25": "1-hour average PM2.5",
            "ave_1hr_pm25_bc": "1-hour average PM2.5 (bias-corrected)",
            "ave_24hr_pm25": "24-hour average PM2.5",
            "ave_24hr_pm25_bc": "24-hour average PM2.5 (bias-corrected)",
            "max_1hr_pm25": "Maximum 1-hour PM2.5",
            "max_1hr_pm25_bc": "Maximum 1-hour PM2.5 (bias-corrected)",
        }

        # Validate domain
        valid_domains = ["CS", "AK", "HI"]
        if self.domain not in valid_domains:
            raise ValueError(f"'domain' must be one of {valid_domains}")

        # Define the AWS S3 path - Note AQM files use a fixed "227" identifier
        # rather than encoding the forecast hour in the filename
        if self.version == "v7":
            # AQMv7 path structure
            s3_path = f"AQMv7/{self.domain}/{self.date:%Y%m%d}/{self.date:%H}/aqm.t{self.date:%H}z.{self.product}.{self.date:%Y%m%d}.227.grib2"
        else:
            # AQMv5 and AQMv6 path structure
            s3_path = f"AQM{self.version}/{self.domain}/{self.date:%Y%m%d}/{self.date:%H}/aqm.t{self.date:%H}z.{self.product}.{self.date:%Y%m%d}.227.grib2"

        self.SOURCES = {
            "aws": f"https://noaa-nws-naqfc-pds.s3.amazonaws.com/{s3_path}",
        }

        self.IDX_SUFFIX = [".idx"]
        self.LOCALFILE = f"{self.get_remoteFileName}"

    def xarray(self, search=None, backend_kwargs=None, remove_grib=True, **download_kwargs):
        """
        Override the default xarray method to handle AQM maximum products.

        For maximum products, simply maps fxx to window index:
        - fxx=0 : first window (index 0)
        - fxx=1 : second window (index 1)
        - fxx=2+ : third window (index 2, or last available)
        """
        # Set default backend kwargs if not provided
        if backend_kwargs is None:
            backend_kwargs = {}

        # Download the file if needed
        local_file = self.download(search, **download_kwargs)

        # For maximum products, disable time handling completely
        if "max" in self.product:
            # Completely disable time handling
            backend_kwargs["decode_times"] = False
            backend_kwargs["filter_by_keys"] = {"typeOfLevel": "surface"}

            # Additional settings to avoid time conversion issues
            backend_kwargs["time_dims"] = []  # Avoid treating any dimension as time

            # Open the dataset with time handling disabled
            ds = xr.open_dataset(
                local_file,
                engine="cfgrib",
                backend_kwargs=backend_kwargs
            )

            # Simple mapping of fxx to window index
            if "time" in ds.dims and len(ds.time) > 1:
                # Map fxx to time index: 0->0, 1->1, 2+->2 (or last index available)
                time_idx = min(self.fxx, 2, len(ds.time)-1)
                ds = ds.isel(time=time_idx)
                ds.attrs["time_window"] = int(time_idx)

            # Add metadata
            ds.attrs["model"] = self.model
            ds.attrs["product"] = self.product
            ds.attrs["domain"] = self.domain

            return ds
        else:
            # For regular (non-max) products, use standard approach
            return super().xarray(
                search=search,
                backend_kwargs=backend_kwargs,
                remove_grib=remove_grib,
                **download_kwargs
            )

    def _load_specific_grib_record(self, file_path, record_number):
        """Load a specific GRIB record from a file by message number."""
        try:
            import cfgrib
            import xarray as xr

            # Use cfgrib index to get message for the specific record
            index = cfgrib.index.FileIndex.from_fileindex(
                file_path,
                indexpath=file_path + '.idx',
                filter_by_keys={}
            )

            # Get all offsets from the index
            all_offsets = index.index_keys.get('offset', [])

            if not all_offsets:
                print("No offsets found in index")
                return None

            # Get the specific offset for this message number
            message_offset = None
            for offset, message in index.index_keys.items():
                if isinstance(message, int) and message == record_number:
                    message_offset = offset
                    break

            if message_offset is None:
                print(f"Could not find offset for message {record_number}")
                return None

            # Open this message only
            ds = xr.open_dataset(
                file_path,
                engine="cfgrib",
                backend_kwargs={
                    "filter_by_keys": {},
                    "encode_cf": "parameter",
                    "grib_errors": "ignore",
                    "decode_times": False,  # Turning off time decoding for maximum products
                    "indexpath": ""
                }
            )

            return ds
        except Exception as e:
            print(f"Error loading specific GRIB record: {e}")
            return None

    def _post_process_dataset(self, ds):
        """Add metadata and custom processing to the dataset."""
        # Add model metadata
        ds.attrs["model"] = "aqm"
        ds.attrs["product"] = self.product
        ds.attrs["domain"] = self.domain
        ds.attrs["version"] = self.version

        # Add product-specific metadata
        if "o3" in self.product:
            if "max" in self.product:
                # Look for OZMAX variables in the dataset
                if "OZMAX1" in ds:
                    ds.attrs["variable"] = "OZMAX1"
                    ds.attrs["units"] = "ppbV"
                    ds.attrs["description"] = "Maximum 1-hour ozone"
                elif "OZMAX8" in ds:
                    ds.attrs["variable"] = "OZMAX8"
                    ds.attrs["units"] = "ppbV"
                    ds.attrs["description"] = "Maximum 8-hour ozone"
            else:
                # For average products, the variable is typically ozcon
                if "ozcon" in ds:
                    ds.attrs["variable"] = "ozcon"
                    ds.attrs["units"] = "ppb"
                    if "1hr" in self.product:
                        ds.attrs["description"] = "1-hour average ozone"
                    else:
                        ds.attrs["description"] = "8-hour average ozone"
        elif "pm25" in self.product:
            if "max" in self.product:
                if "PMMAX" in ds:
                    ds.attrs["variable"] = "PMMAX"
                    ds.attrs["units"] = "µg/m³"
                    ds.attrs["description"] = "Maximum PM2.5"
            else:
                if "pmtf" in ds:
                    ds.attrs["variable"] = "pmtf"
                    ds.attrs["units"] = "µg/m³"
                    if "1hr" in self.product:
                        ds.attrs["description"] = "1-hour average PM2.5"
                    else:
                        ds.attrs["description"] = "24-hour average PM2.5"

        # Document our processing
        ds.attrs["processing"] = f"Processed with Herbie AQM module for forecast hour {self.fxx}"

        return ds