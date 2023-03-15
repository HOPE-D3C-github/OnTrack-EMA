# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary:  Combine the curated lists of enrolled participants from MDA and Utah
#     
# Inputs:   file.path(path_ontrack_visit_outputs, "enrolled_mda.RDS")
#           file.path(path_ontrack_visit_outputs, "utah_data_all.rds")
#     
# Outputs:  file.path(path_ontrack_visit_outputs, "masterlist.RData")
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(dplyr)
library(lubridate)
library(purrr)

source("paths.R")

dat_master_mda <- readRDS(file.path(path_ontrack_visit_outputs, "enrolled_mda.RDS")) %>% 
  select(ID_MDA, participant_id, ID_enrolled, cc_indicator, cohort, in_ematimes, withdrew, withdrawn_date, v1_date_curated,
         event_v1_EventStatus, event_v2_EventDate, event_v2_EventStatus,event_v3_EventDate, 
         event_v3_EventStatus, v4_date_curated, event_v4_EventStatus) %>% 
  rename(withdrew_date = withdrawn_date,
         v1_date = v1_date_curated,
         v1_attend = event_v1_EventStatus,
         v2_date = event_v2_EventDate,
         v2_attend = event_v2_EventStatus,
         v3_date = event_v3_EventDate,
         v3_attend = event_v3_EventStatus,
         v4_date = v4_date_curated,
         v4_attend = event_v4_EventStatus) %>% 
  mutate(participant_id_2 = case_when(
    ID_MDA == '530079' ~ 'ses_119',
    T ~ NA_character_
  ), .after = participant_id) %>% 
  mutate(ID_RSR = NA_character_, .before = everything()) %>% 
  mutate(enrollment_site = 'MDA', .before = everything())

dat_master_utah <- readRDS(file = file.path(path_ontrack_visit_outputs, "utah_data_all.rds")) %>% 
  filter(enrolled) %>% 
  mutate(cohort = case_when(
    sgmeligible == 1 ~ "SGM",
    T ~ "SES"
  )) %>% 
  select(ID_RSR, participant_id, participant_id_2, ID_enrolled, cc_indicator, cohort, in_ematimes, withdrew_calc, withdrew_date_calc, v1_date_calc, v1_attend_calc, v2_date_calc, 
         v2_attend_ind_calc, v3_date_calc, v3_attend_ind_calc, v4_date_calc, v4_attend_ind_calc, withdrew_calc, withdrew_date_calc) %>% 
  rename(withdrew = withdrew_calc,
         withdrew_date = withdrew_date_calc,
         v1_date = v1_date_calc,
         v1_attend = v1_attend_calc,
         v2_date = v2_date_calc,
         v2_attend = v2_attend_ind_calc,
         v3_date = v3_date_calc,
         v3_attend = v3_attend_ind_calc,
         v4_date = v4_date_calc,
         v4_attend = v4_attend_ind_calc) %>% 
  mutate(ID_MDA = NA_character_, .after = ID_RSR)%>% 
  mutate(enrollment_site = 'Utah', .before = everything())


dat_master <- dat_master_utah %>% rbind(dat_master_mda)

# --------------------------------------
# modifying code from BreakFree
# also starting to include "local" time
# which will be forced timezone of UTC
# --------------------------------------
dat_master <- dat_master %>% 
  mutate(timezone_local = ifelse(enrollment_site == 'Utah', "America/Denver", "America/Chicago"))

dat_master <- dat_master %>%
  mutate(begin_date_ema_data_collection = v1_date)

dat_master <- dat_master %>% arrange(cc_indicator, ID_RSR, ID_MDA)

# Now, construct time variables
dat_master <- dat_master %>%
  mutate(first_day_hrts_local = force_tz(begin_date_ema_data_collection, tzone = "UTC"),
         first_day_date = as_date(first_day_hrts_local))

dat_master <- dat_master %>%
  mutate(quit_day_hrts_local = first_day_hrts_local + days(4),
         last_day_hrts_local = first_day_hrts_local + days(13), #14 days of study
         last_day_date = as_date(last_day_hrts_local))

hour(dat_master$quit_day_hrts_local) <- 4

dat_master <- dat_master %>%
  mutate( 
    first_day_unixts = map2(first_day_hrts_local, timezone_local, ~force_tz(.x, .y)) %>% as.double(.),
    quit_day_unixts = map2(quit_day_hrts_local, timezone_local, ~force_tz(.x, .y)) %>% as.double(.),
    last_day_unixts = map2(last_day_hrts_local, timezone_local, ~force_tz(.x, .y)) %>% as.double(.)
  )

# Clean up data frame
dat_master <- dat_master %>% 
  select(-begin_date_ema_data_collection) %>%
  select(ID_enrolled, ID_RSR, ID_MDA, participant_id, 
         participant_id_2, cc_indicator, cohort, enrollment_site, in_ematimes,
         withdrew, withdrew_date, first_day_date, last_day_date,
         quit_day_unixts, quit_day_hrts_local, timezone_local,
         v1_date, v1_attend, v2_date, v2_attend, v3_date, v3_attend, v4_date, v4_attend)

# Save to RData file
save(dat_master, file = file.path(path_ontrack_visit_outputs, "masterlist.RData"))

