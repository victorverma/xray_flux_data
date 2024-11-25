here::i_am("scripts/download_1m_data.R")

# Load packages -----------------------------------------------------------

library(curl)
library(here)
library(parallel)
library(tidyverse)

# Define functions --------------------------------------------------------

download_data <- function(url, satellite) {
  destfile <- str_glue(here("data/raw/1m/{satellite}.nc"))
  try(curl_download(url, destfile))
}

# Download the data -------------------------------------------------------

# The files below were found by following links from 
# https://www.ngdc.noaa.gov/stp/satellite/goes-r.html, which links to
# repositories of science-quality data. The links to follow are those for the
# "1-minute Averages" product of the XRS instrument.
# 
# The documentation at
# https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes16/l1b/docs/GOES-R_EXIS_XRS_L1b_Data_ReadMe.pdf
# recommends using science-quality L2 data instead of science-quality L1b data
urls <- c(
  goes08 = "https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/science/xrs/goes08/xrsf-l2-avg1m_science/sci_xrsf-l2-avg1m_g08_s19950103_e20030616_v1-0-0.nc",
  goes09 = "https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/science/xrs/goes09/xrsf-l2-avg1m_science/sci_xrsf-l2-avg1m_g09_s19960401_e19980728_v1-0-0.nc",
  goes10 = "https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/science/xrs/goes10/xrsf-l2-avg1m_science/sci_xrsf-l2-avg1m_g10_s19980701_e20091201_v1-0-0.nc",
  goes11 = "https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/science/xrs/goes11/xrsf-l2-avg1m_science/sci_xrsf-l2-avg1m_g11_s20060601_e20080210_v1-0-0.nc",
  goes12 = "https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/science/xrs/goes12/xrsf-l2-avg1m_science/sci_xrsf-l2-avg1m_g12_s20030110_e20070412_v1-0-0.nc",
  goes13 = "https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/science/xrs/goes13/xrsf-l2-avg1m_science/sci_xrsf-l2-avg1m_g13_s20130607_e20171214_v2-2-0.nc",
  goes14 = "https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/science/xrs/goes14/xrsf-l2-avg1m_science/sci_xrsf-l2-avg1m_g14_s20090919_e20200304_v2-2-0.nc",
  goes15 = "https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/science/xrs/goes15/xrsf-l2-avg1m_science/sci_xrsf-l2-avg1m_g15_s20100407_e20200304_v2-2-0.nc",
  goes16 = "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes16/l2/data/xrsf-l2-avg1m_science/sci_xrsf-l2-avg1m_g16_s20170207_e20241122_v2-2-0.nc",
  goes17 = "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes17/l2/data/xrsf-l2-avg1m_science/sci_xrsf-l2-avg1m_g17_s20180601_e20230110_v2-2-0.nc",
  goes18 = "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes18/l2/data/xrsf-l2-avg1m_science/sci_xrsf-l2-avg1m_g18_s20220902_e20241122_v2-2-0.nc"
)

mcmapply(download_data, urls, names(urls), mc.cores = detectCores()) %>%
  invisible()