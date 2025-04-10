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


class aqm:
    def template(self):
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
            # Default to current operational version (v7 as of May 2024)
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

        self.EXPECT_IDX_FILE = None
        self.IDX_SUFFIX = [".idx"]
        self.LOCALFILE = f"{self.get_remoteFileName}"