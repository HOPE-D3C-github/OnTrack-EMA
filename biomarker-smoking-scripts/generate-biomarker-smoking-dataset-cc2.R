# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary:  Combine participant-level CC2 biomarker dataframes and apply filtering
#             criteria based on active study-period and de-duplication
#     
# Inputs:   file.path(path_ontrack_ema_staged, "masterlist_updated.RDS")
#           file.path(path_ontrack_ema_staged, "online_puffmarker_episode_raw_data_cc2.RData")
#     
# Outputs:  file.path(path_ontrack_ema_staged, "online_puffmarker_episode_data_cc2.RData")
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(dplyr)
library(lubridate)
library(purrr)
source("paths.R")

dat_master <- readRDS(file.path(path_ontrack_ema_staged, "masterlist_updated.RDS"))
load(file.path(path_ontrack_ema_staged, "online_puffmarker_episode_raw_data_cc2.RData"))

df_raw <- do.call(rbind, online_puffmarker_episode_files_cc2)

# -----------------------------------------------------------------------------
# Crosswalk participant_id to ID_enrolled and get local timezone
# -----------------------------------------------------------------------------
crosswalk <- dat_master %>% filter( cc_indicator == 2 ) %>% select(ID_enrolled, participant_id, timezone_local, cc_indicator, first_day_date, last_day_date) %>%
  add_row(dat_master %>% filter( cc_indicator == 2 & !is.na(participant_id_2) ) %>% 
            select(ID_enrolled, participant_id = participant_id_2, timezone_local, cc_indicator, first_day_date, last_day_date))


df_analysis <- df_raw %>% 
  left_join( y = crosswalk,
             by = "participant_id",
             multiple = "all")

df_analysis <- df_analysis %>% filter(!is.na(ID_enrolled)) 

df_analysis <- df_analysis %>% relocate(ID_enrolled, timezone_local, .before = participant_id) %>% select(-participant_id)


# -----------------------------------------------------------------------------
# Construct time variables
# -----------------------------------------------------------------------------
df_analysis <- df_analysis %>%
  mutate(onlinepuffm_unixts = V1/1000,
         onlinepuffm_hrts_UTC = as.POSIXct(onlinepuffm_unixts, tz = "UTC", origin="1970-01-01"), 
         onlinepuffm_hrts_local = map2(onlinepuffm_hrts_UTC, timezone_local, ~with_tz(.x,.y) %>% force_tz(., "UTC")) %>% unlist %>% as_datetime(.)
  ) %>%
  select(ID_enrolled, cc_indicator, timezone_local, onlinepuffm_unixts, onlinepuffm_hrts_UTC, onlinepuffm_hrts_local, first_day_date, last_day_date)

# -----------------------------------------------------------------------------
# Apply our exclusion rules
# -----------------------------------------------------------------------------
df_analysis <- df_analysis %>% 
  filter(onlinepuffm_hrts_local >= first_day_date) %>%
  filter(onlinepuffm_hrts_local <= last_day_date) %>%
  select(-first_day_date, -last_day_date) 

# -----------------------------------------------------------------------------
# Drop duplicate records
# -----------------------------------------------------------------------------
idx_duplicates <- df_analysis %>%
  select(ID_enrolled, onlinepuffm_unixts) %>%
  duplicated(.)

# -----------------------------------------------------------------------------
# Save Output
# -----------------------------------------------------------------------------
df_analysis <- df_analysis %>% filter(!idx_duplicates)

online_puffmarker_episode_data_cc2 <- df_analysis
save(online_puffmarker_episode_data_cc2, file = file.path(path_ontrack_ema_staged, "online_puffmarker_episode_data_cc2.RData"))

