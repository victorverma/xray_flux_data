here::i_am("scripts/process_1m_data.R")

# Load packages -----------------------------------------------------------

library(arrow)
library(here)
library(lubridate)
library(ncdf4)
library(tidyverse)
library(tools)

# Define functions --------------------------------------------------------

make_goes_flux_tbl <- function(file, complete = TRUE) {
  netcdf_obj <- nc_open(file)
  
  # If this definition were outside the body of this function, it would have to
  # take netcdf_obj as an argument. netcdf_obj may be large; I don't know
  # whether it would be copied in that scenario, and I don't know if it would be
  # safe to copy it
  make_var_tbl <- function(var) {
    # Using as.vector() is necessary because ncvar_get() returns a 1D array
    tibble("{var}" := as.vector(ncvar_get(netcdf_obj, var)))
  }
  
  goes_flux_tbl <- netcdf_obj %>%
    `$`("var") %>%
    names() %>%
    # Use XRS-B (long channel) data, not XRS-A (short channel) data, as the
    # magnitude of a flare is based on the former
    str_subset("^xrsb_") %>%
    map(make_var_tbl) %>%
    list_cbind() %>%
    rename_with(str_remove, pattern = "^xrsb_") %>%
    # For GOES-13 and later satellites, the word "flag" is used, but for earlier
    # satellites, the word "flags" is used
    rename_with(str_replace, pattern = fixed("flags"), replacement = "flag")

  # Extract metadata needed for interpreting the flag values in the data. This 
  # approach is based on a recommendation from Janet Machol of NOAA
  flag_meanings <- NULL
  flag_masks <- NULL
  flag_values <- NULL
  netcdf_obj %>%
    {
      tryCatch(
        ncatt_get(., "xrsb_flag"),
        error = function(e) ncatt_get(., "xrsb_flags")
      )
    } %>%
    with(
      {
        flag_meanings <- unlist(str_split(flag_meanings, " "))
        flag_meanings <<- flag_meanings
        flag_masks <<- set_names(flag_masks, flag_meanings)
        flag_values <<- set_names(flag_values, flag_meanings)
      }
    )
  for (flag_meaning in flag_meanings) {
    flag_mask <- flag_masks[flag_meaning]
    flag_value <- flag_values[flag_meaning]
    goes_flux_tbl <- mutate(
      goes_flux_tbl,
      # This computation is based on the information at 
      # https://cfconventions.org/cf-conventions/cf-conventions.html#flags
      "{flag_meaning}" := bitwAnd(flag, flag_mask) == flag_value
    )
  }
  # Another good source of information about working with the flags is
  # https://cires-stp.github.io/goesr-spwx-examples/examples/exis/ncflag_example.html#sphx-glr-examples-exis-ncflag-example-py,
  # but it uses 2s data
  
  # Per the help page for ncvar_get(), this is the usual approach to extracting
  # dimension values
  times <- with(
    netcdf_obj$dim$time,
    {
      # units should be a string of the form
      # "seconds since yyyy-mm-dd hh:mm:ss"
      units %>%
        str_remove("^seconds since ") %>%
        ymd_hms() %>%
        `+`(seconds_to_period(vals))
    }
  )
  goes_flux_tbl <- add_column(goes_flux_tbl, time = times, .before = 1)
  
  nc_close(netcdf_obj)
  
  # For some reason, there are no observations for some minutes. This inserts
  # observations for those minutes
  if (complete) {
    goes_flux_tbl <- complete(goes_flux_tbl, time = full_seq(time, period = 60))
  }
  goes_flux_tbl
}

# Process the data --------------------------------------------------------

# (1) is a readme for science-quality GOES 13-15 data. (2) and (3) have 
# information about GOES 16-18 data; they also have information specifically
# about the minutely data. Variable descriptions, units, and other information
# can be obtained from the "var" component of the object returned by
# ncdf4::nc_open().
#
# (1) https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/science/xrs/GOES_1-15_XRS_Science-Quality_Data_Readme.pdf
# (2) https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes16/l2/docs/GOES-R_XRS_L2_Data_Users_Guide.pdf
# (3) https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes16/l2/docs/GOES-R_XRS_L2_Data_Readme.pdf

# For each satellite, extract the data from it into a tibble
goes_flux_tbl_nms <- character()
for (file in list.files(here("data/raw/1m/"), full.names = TRUE)) {
  goes_flux_tbl_nm <- file %>%
    basename() %>%
    file_path_sans_ext() %>%
    str_c("flux_tbl", sep = "_")
  goes_flux_tbl_nms <- c(goes_flux_tbl_nms, goes_flux_tbl_nm)
  assign(goes_flux_tbl_nm, make_goes_flux_tbl(file))
}

# At any time, one GOES satellite is supposed to be the primary satellite and
# another is supposed to be the secondary satellite. Data from the secondary
# satellite is used when the primary satellite is being eclipsed or under
# maintenance, otherwise primary data should be used. The information in the
# tibble below is from Table 6 in (1). In the table, if the transition time is
# midnight, then the exact time is actually unknown. Janet Machol of NOAA
# clarified some features of the table for me. If a primary/secondary entry is
# blank, then there was no change from the previous transition time. For
# example, in the table, the primary entry for 2016-05-16 17:00 is blank; this
# means that the primary satellite didn't change at that time - it continued to
# be GOES-14. If a secondary entry is "None", then there was no secondary
# satellite at the corresponding time. I noticed some discrepancies between
# Table 6 of (1) and Table 2 of (2). Dr. Machol recommended using (1) since it
# is for science-quality data, which should be used, while (2) is for
# operational data, and is also not as up-to-date.
#
# (1) https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/science/xrs/GOES_1-15_XRS_Science-Quality_Data_Readme.pdf
# (2) https://ngdc.noaa.gov/stp/satellite/goes/doc/GOES_XRS_readme.pdf
primary_secondary_tbl <- tribble(
  ~start_time, ~primary_satellite, ~secondary_satellite,
  ymd_hm("2023-01-04 00:00"), 16, 18,
  ymd_hm("2018-01-06 00:00"), 16, 17,
  ymd_hm("2017-02-07 00:00"), 16, 15,
  ymd_hm("2016-06-09 17:30"), 15, 13,
  ymd_hm("2016-05-16 17:00"), 14, 15,
  ymd_hm("2016-05-12 17:30"), 14, 13,
  ymd_hm("2016-05-03 13:00"), 13, 14,
  ymd_hm("2015-06-09 16:25"), 15, 13,
  ymd_hm("2015-05-21 18:00"), 14, 13,
  ymd_hm("2015-01-26 16:01"), 15, 13,
  ymd_hm("2012-11-19 16:31"), 15, 14,
  ymd_hm("2012-10-23 16:00"), 14, 15,
  ymd_hm("2011-09-01 00:00"), 15, 14,
  ymd_hm("2010-10-28 00:00"), 15, NA_integer_,
  ymd_hm("2010-09-01 00:00"), 14, 15,
  ymd_hm("2009-12-01 00:00"), 14, NA_integer_,
  ymd_hm("2008-02-10 16:30"), 10, NA_integer_,
  ymd_hm("2007-04-12 00:00"), 11, 10,
  ymd_hm("2007-01-01 00:00"), 10, 11,
  ymd_hm("2006-06-28 00:00"), 12, 11,
  ymd_hm("2003-05-15 15:00"), 12, 10,
  ymd_hm("2003-04-08 15:00"), 10, 12,
  ymd_hm("1998-07-27 00:00"), 8, 10,
  ymd_hm("1995-03-01 00:00"), 8, 7,
  ymd_hm("1994-12-11 00:00"), 7, 8,
  ymd_hm("1988-01-26 00:00"), 7, 6,
  ymd_hm("1986-01-01 00:00"), 6, 5
) %>%
  mutate(
    end_time = c(
      ymd_hms("9999-12-31 11:59:59"), head(start_time, -1) - seconds(1)
    )
  ) %>%
  relocate(end_time, .after = start_time)

# Stack the tibbles for the individual satellites
flux_tbl <- goes_flux_tbl_nms %>%
  mget(.GlobalEnv) %>%
  map(~ select(.x, time, flux, good_data)) %>%
  list_rbind(names_to = "satellite") %>%
  mutate(satellite = str_extract(satellite, "\\d+")) %>%
  arrange(time)

# Keep the records that are from a primary or secondary satellite
flux_tbl <- flux_tbl %>%
  inner_join(
    primary_secondary_tbl,
    join_by(between(time, start_time, end_time))
  ) %>%
  mutate(
    rec_type = case_when(
      satellite == primary_satellite ~ "primary",
      satellite == secondary_satellite ~ "secondary"
    )
  ) %>%
  select(!c(start_time, end_time, primary_satellite, secondary_satellite)) %>%
  filter(!is.na(rec_type))

# For each time, combine the associated records
flux_tbl <- flux_tbl %>%
  pivot_wider(
    id_cols = time,
    names_from = rec_type,
    values_from = c(satellite, flux, good_data)
  ) %>%
  mutate(
    satellite = case_when(
      good_data_primary ~ satellite_primary,
      good_data_secondary ~ satellite_secondary
    ),
    flux = case_when(
      good_data_primary ~ flux_primary, good_data_secondary ~ flux_secondary
    ),
    good_data = coalesce(good_data_primary | good_data_secondary, FALSE)
  )

# Save the processed data -------------------------------------------------

write_parquet(flux_tbl, here("data/processed/1m_data.parquet"))