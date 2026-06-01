# xray_flux_data
Code for downloading and processing solar X-ray flux data.

## Data preparation

Before preparing the data, open the project in RStudio using the `.Rproj` file.

To download the raw 1-minute data to [`data/raw/1m/`](data/raw/1m/), source [`scripts/download_1m_data.R`](scripts/download_1m_data.R). Before running the script, the URLs may need to be updated.

```r
source("scripts/download_1m_data.R")
```

To create a Parquet file containing all of the data and save it to [`data/processed/`](data/processed/), source [`scripts/process_1m_data.R`](scripts/process_1m_data.R). Before running the script, the code that creates `primary_secondary_tbl` may need to be updated.

```r
source("scripts/process_1m_data.R")
```

See the comments in [`scripts/download_1m_data.R`](scripts/download_1m_data.R) and [`scripts/process_1m_data.R`](scripts/process_1m_data.R) for more information.
