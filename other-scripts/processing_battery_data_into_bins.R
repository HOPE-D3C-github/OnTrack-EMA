# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary:  Combine the participant-level phone battery dataframes and apply binning
#             to create a dataset with start and end time for categories of battery status
#     
# Inputs:   file.path(path_ontrack_visit_outputs, "masterlist.RData")
#           file.path(path_ontrack_ema_staged, "filtered_battery_data.RData")
#     
# Outputs:  file.path(path_ontrack_ema_staged, "battery_data_binned.RData")
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(dplyr)
library(lubridate)
library(tidyr)
library(purrr)

source("paths.R")

load(file.path(path_ontrack_ema_staged, "filtered_battery_data.RData"))

load(file.path(path_ontrack_visit_outputs, "masterlist.RData"))

filtered_battery_data <- filtered_battery_data %>% 
  mutate(
    bat_bin = case_when(
      lag_diff_secs > 300 ~ "No Battery Data",
      battery_percent <= 100 & battery_percent > 10 ~ "10-100%",
      battery_percent <= 10 & battery_percent >= 0 ~ "0-10%"
    )
  )

# -------------------------------------------------------
# Use the bat_bin variable to reduce down to time ranges in each category
# -------------------------------------------------------

# Shell to add to with each participant
battery_binned_all <- data.frame(
  participant_id = character(),
  unix_datetime_start = integer(),
  unix_datetime_end = integer(),
  datetime_hrts_UTC_start = as_datetime(character()),
  datetime_hrts_UTC_end = as_datetime(character()),
  battery_status = character(),
  time_duration_minutes = numeric()
) 

for (participant in dat_master$participant_id){
  print(participant)
  dat_battery_participant <- filtered_battery_data %>% filter(participant_id == participant)
  
  # Add a lead variable to see if the battery status changed from the current record to the next
  dat_battery_participant <- dat_battery_participant  %>% mutate(bat_bin_lead = lead(bat_bin),
                                                                       bat_bin_lag = lag(bat_bin))
  
  dat_battery_participant <- dat_battery_participant %>% 
    filter(bat_bin != bat_bin_lead | bat_bin != bat_bin_lag | row_number() == 1 | row_number()==nrow(.)) %>% 
    rename(
      unix_datetime_start = datetime,
      datetime_hrts_UTC_start = datetime_hrts_UTC,
      battery_status = bat_bin
    ) %>% 
    mutate(
      unix_datetime_end = lead(unix_datetime_start),
      datetime_hrts_UTC_end = lead(datetime_hrts_UTC_start), 
      .after = datetime_hrts_UTC_start
    ) %>% select(-lag_diff_secs, -lag_diff_battery_percent, -lead_diff_battery_percent, -lead_battery_percent, -battery_status) %>% 
    rename(battery_status = bat_bin_lead) %>% 
    mutate(
      unix_datetime_end = case_when(
        row_number() == nrow(.) ~ 1640995201,
        T ~ unix_datetime_end),
      datetime_hrts_UTC_end = case_when(
        row_number() == nrow(.) ~ as_datetime(1640995201, origin = lubridate::origin),
        T ~ datetime_hrts_UTC_end),
      battery_status = case_when(
        row_number() == nrow(.) ~ "No Battery Data",
        T ~ battery_status)
    )
  
  dat_battery_participant <- dat_battery_participant %>% 
    add_row(
      data.frame(
        participant_id = participant, unix_datetime_start = 1262304001, battery_percent = NA, datetime_hrts_UTC_start = as_datetime(1262304001, origin = lubridate::origin),
        unix_datetime_end = dat_battery_participant$unix_datetime_start[1], datetime_hrts_UTC_end = dat_battery_participant$datetime_hrts_UTC_start[1],
        lag_battery_percent = NA, battery_status = "No Battery Data", bat_bin_lag = NA), 
      .before = 1
    ) %>% select(-lag_battery_percent, -bat_bin_lag, -battery_percent)
  
  # Add variable for the time duration in minutes spanned by the battery status 
  dat_battery_participant <- dat_battery_participant %>% 
    mutate(
      time_duration_minutes = round(time_length(datetime_hrts_UTC_end - datetime_hrts_UTC_start, "minutes"), digits = 1)
    )
  
  # Add the participant's data into the dataframe for all participants
  battery_binned_all <- battery_binned_all %>% 
    add_row(dat_battery_participant)
}

crosswalk_pt_id_site <- dat_master %>% select(ID_enrolled, participant_id, cc_indicator, enrollment_site, timezone_local) %>% 
  add_row(dat_master %>% filter(!is.na(participant_id_2)) %>% select(ID_enrolled, participant_id_2,  cc_indicator, enrollment_site, timezone_local) %>% rename(participant_id = participant_id_2))

battery_binned_all <- crosswalk_pt_id_site  %>% 
  right_join(y = battery_binned_all,
            by = 'participant_id')

# @TB: replace AmerChi timezone with "local"
battery_binned_all <- battery_binned_all %>% 
  rename(battery_unix_datetime_start = unix_datetime_start,
         battery_unix_datetime_end = unix_datetime_end,
         battery_hrts_UTC_start = datetime_hrts_UTC_start,
         battery_hrts_UTC_end = datetime_hrts_UTC_end,
         battery_time_duration_minutes = time_duration_minutes) %>% 
  mutate(
    battery_hrts_local_start = map2(battery_hrts_UTC_start, timezone_local, ~with_tz(.x,.y) %>% force_tz(., "UTC")) %>% unlist %>% as_datetime(.),
    battery_hrts_local_end = map2(battery_hrts_UTC_end, timezone_local, ~with_tz(.x,.y) %>% force_tz(., "UTC")) %>% unlist %>% as_datetime(.), 
    .after = battery_unix_datetime_end) %>% 
  arrange(ID_enrolled)

save(battery_binned_all,
     file = file.path(path_ontrack_ema_staged, "battery_data_binned.RData"))
