# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary:  Compile the EMA timestamps data from CC1 and CC2, then collect the 
#             first and last EMA timestamp for each participant
#     
# Inputs:   file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData")
#           file.path(path_ontrack_ema_staged, "ema_raw_data_cc2.RData")
#     
# Outputs:  file.path(path_ontrack_visit_staged, "ema_first_last_dates.rds")
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(tidyverse)
library(lubridate)

source("paths.R")

load(file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData"))
cc1_times <- map(all_random_ema_response_files_cc1, ~  select(., participant_id,mCerebrum_timestamp)) %>% bind_rows %>% 
  mutate(unix_timestamp = mCerebrum_timestamp/1000,
         # begin_hrts_UTC = as.POSIXct(unix_timestamp, tz = "UTC", origin="1970-01-01"),
         # begin_hrts_AmericaChicago = with_tz(begin_hrts_UTC, tzone = "America/Chicago")
         ) %>% 
  mutate(cc_indicator = "1") %>% 
  select(participant_id, cc_indicator, unix_timestamp)


load(file.path(path_ontrack_ema_staged, "ema_raw_data_cc2.RData"))
cc2_times <- all_random_ema_status_files_cc2 %>% as_tibble %>% 
  mutate(unix_timestamp = as.numeric(V1)/1000,
         #begin_hrts_UTC = as.POSIXct(unix_timestamp, tz = "UTC", origin="1970-01-01"),
         #begin_hrts_AmericaChicago = with_tz(begin_hrts_UTC, tzone = "America/Chicago")
         ) %>% 
  mutate(cc_indicator = "2") %>% 
  select(participant_id, cc_indicator, unix_timestamp)

ema_times <- bind_rows(cc1_times,cc2_times) %>% 
  #arrange(participant_id, unix_timestamp) %>% 
  group_by(participant_id, cc_indicator) %>% 
  summarise(unix_ts_first = min(unix_timestamp), unix_ts_last = max(unix_timestamp)) %>% 
  mutate(
    hrts_UTC_first = as.POSIXct(unix_ts_first, tz = "UTC", origin="1970-01-01"),
    hrts_UTC_last = as.POSIXct(unix_ts_last, tz = "UTC", origin="1970-01-01"),
    hrts_AmericaChicago_first = with_tz(hrts_UTC_first, tzone = "America/Chicago"),
    hrts_AmericaChicago_last = with_tz(hrts_UTC_last, tzone = "America/Chicago")
  )

saveRDS(object = ema_times, file = file.path(path_ontrack_visit_staged, "ema_first_last_dates.rds"))

