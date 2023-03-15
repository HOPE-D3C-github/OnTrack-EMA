# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary ####
# Step 1: Identify Undelivered for No Day Start, End of Day before Block Start, and After Withdrew Date
#     1A: Identify End of Day Before Block Start
#     1B: Identify blocks for which there was no Day Start
#     1C: Identify blocks which occurred after the participant's withdrew date (if they withdrew)
#
# Step 2: Join Battery Data with Undelivered EMAs
#     2A: Prep Undelivered Dataset / Hold Delivered Dataset Seperately Until Recombining to occur after Reconciling Undelivered Rsn
#     2B: Execute the Overlap Join
#     2C: Aggregating battery_status for blocks with multiple battery_status matches
#     2D: Update Battery Status for Participants Missing All Battery Data
# 
# Step 3: Start Labeling Undelivered Reason using Hierarchy (from top down to where need conditions log)
# 
# Step 4: Join Phone Status Data with remaining unreconciled Undelivered EMAs
#     4A: Pre-process the phone status data for the join
#     4B: Execute the 'Between' Join
#     4C: Post-join Cleaning
#     4D: Identify the reason for undelivery when able to get that from the phone log
#     4E: Identify the reason for undelivery for the remaining EMAs
# 
# Step 5: Use undelivered reason to update the status variable
# 
# Step 6: Update statuses for delivered EMAs
# 
# Step 7: Recombine undelivered and delivered data and save dataset
#
# 
# Inputs:   file.path(path_ontrack_ema_staged, "block_level_ema_dataset_pre-undelivered_rsn.RData")
#           file.path(path_ontrack_ema_staged, "masterlist_updated.RDS")
#           file.path(path_ontrack_ema_staged, "battery_data_binned.RData")
#           file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData")
#           
# Outputs:  file.path(path_ontrack_ema_staged, "block_level_ema_dataset_cc1.RDS")
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
library(dplyr)
library(tidyr)
library(data.table)
library(testthat)
library(lubridate)
library(purrr)
library(rlang)

source("paths.R")
load(file = file.path(path_ontrack_ema_staged, "block_level_ema_dataset_pre-undelivered_rsn.RData"))
remove(block_level_ema_cc2)
dat_master <- readRDS(file.path(path_ontrack_ema_staged, "masterlist_updated.RDS"))
load(file.path(path_ontrack_ema_staged, "battery_data_binned.RData"))
battery_binned_cc1 <- battery_binned_all %>% filter(cc_indicator == 1)
remove(battery_binned_all)
load(file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData"))
phone_status_cc1 <- bind_rows(all_log_phone_files_cc1)
remove(all_day_start_files_cc1, all_day_end_files_cc1, all_random_ema_response_files_cc1, all_smoking_ema_response_files_cc1, 
       all_stress_ema_response_files_cc1, dat_file_counts_cc1, all_log_phone_files_cc1)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1: Identify Undelivered for No Day Start, End of Day before Block Start, and After Withdrew Date ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1A: Identify End of Day Before Block Start ####
#   * Identify End of Block times when concatenated due to an End of Day button press before the end of a block
#   * Identify blocks where the End of Day button was pressed before the start of the block
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

block_level_ema_cc1 <- block_level_ema_cc1 %>% relocate(status, begin_hrts_local, end_hrts_local, block_start_hrts_local, block_end_hrts_local, day_start_hrts_local, day_end_hrts_local, .after = block)

block_level_ema_cc1 <- block_level_ema_cc1 %>% 
  mutate(
    end_of_day_before_block_start = case_when(
      is.na(coalesce(day_end_hrts_local_2, day_end_hrts_local)) ~ FALSE,  # No End of Day recorded for that study day
      coalesce(day_end_hrts_local_2, day_end_hrts_local) < block_start_hrts_local ~ TRUE,   # Last End of Day recorded for that study day was before the start of the respective block
      T ~ FALSE
    ), .before = block_start_hrts_local
  )

block_level_ema_cc1 <- block_level_ema_cc1 %>% 
  mutate(
    end_of_day_during_block = case_when(
      end_of_day_before_block_start ~ FALSE,
      coalesce(day_end_hrts_local_2, day_end_hrts_local) < block_end_hrts_local ~ TRUE,
      T ~ FALSE
    ), .after = block_end_hrts_local)

# @TB: Do we update end block times for early end of day button presses? If so, how to treat block start and block end when end_of_day_before_block_start == TRUE?

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1B: Identify blocks for which there was no Day Start ####
#   
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
block_level_ema_cc1 <- block_level_ema_cc1 %>% 
  mutate(
    no_day_start_on_study_day = is.na(day_start_hrts_local)
  )

if(F){block_level_ema_cc1 %>% count(no_day_start_on_study_day, status)}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1C: Identify blocks which occurred after the participant's withdrew date (if they withdrew) ####
#   * @TB: Did we censor EMAs on the withdrew date, or only study days after the withdrew date for Break Free?
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if(F){block_level_ema_cc1 %>% mutate(on_withdrew_date = study_date == withdrew_date) %>% View}
if(F){block_level_ema_cc1 %>% mutate(on_withdrew_date = study_date == withdrew_date, .after = study_date) %>% 
    relocate(withdrew_date, .after = study_date) %>% filter(on_withdrew_date) %>% count(status)}

block_level_ema_cc1 <- block_level_ema_cc1 %>% mutate(
  after_pt_withdrew = coalesce(study_date > withdrew_date, FALSE)  # Many pt did not have a withdrew date, which yields an NA response. Logically, we want that to be a FALSE value so added coalesce()
) 

if(F){block_level_ema_cc1 %>% count(after_pt_withdrew, status)}



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2: Join Battery Data  with Undelivered EMAs ####
#     2A: Prep Undelivered Dataset / Hold Delivered Dataset Seperately Until Recombining to occur after Reconciling Undelivered Rsn
#     2B: Execute the Overlap Join
#     2C: Aggregating battery_status for blocks with multiple battery_status matches
#     2D: Update Battery Status for Participants Missing All Battery Data
#
#   NOTES:
#   * Underlivered EMAs have a block-length time interval for when the EMA could've been delivered
#   * Using an interval join where we match all battery statuses with the time interval (block length)   
#   * Testing out dplyr's new overlap joins, specifically using the 'overlaps' argument
#     - https://dplyr.tidyverse.org/reference/join_by.html#overlap-joins
#   * Battery Status with a value of NA represents a day without a day start button press
#           - Days without a day start button press have a higher ranking towards undelivered reason
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2A: Prep Undelivered Dataset / Hold Delivered Dataset Seperately Until Recombining to occur after Reconciling Undelivered Rsn  ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

battery_binned_cc1 <- battery_binned_cc1 %>% 
  select(ID_enrolled, battery_hrts_local_start, battery_hrts_local_end, battery_status)


ema_dat_delivered <- block_level_ema_cc1 %>% filter(status != 'UNDELIVERED') %>% mutate(ema_delivered = TRUE)
ema_dat_undelivered <- block_level_ema_cc1 %>% filter(status == 'UNDELIVERED') %>% mutate(ema_delivered = FALSE)

test_1 <- test_that(desc = 'Test that the split grabbed all the rows from block_level_ema_cc1', {
  expect_equal(object = nrow(ema_dat_delivered) + nrow(ema_dat_undelivered),
               expected = nrow(block_level_ema_cc1))
})

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2B: Execute the Overlap Join ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if(test_1){
  by <- join_by( ID_enrolled, overlaps( x$block_start_hrts_local, x$block_end_hrts_local, y$battery_hrts_local_start, y$battery_hrts_local_end ) )
}

ema_dat_undelivered <- left_join( x = ema_dat_undelivered, y = battery_binned_cc1, by)
remove(by)
ema_dat_undelivered <- ema_dat_undelivered %>% relocate(battery_status, battery_hrts_local_start, battery_hrts_local_end, .after = block_end_hrts_local)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2C: Aggregating battery_status for blocks with multiple battery_status matches ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ema_dat_undelivered <- ema_dat_undelivered %>% 
  group_by(ID_enrolled, study_day_int, block, ema_type) %>% 
  mutate(n_bat=n(), all_battery_statuses = list(unique(battery_status)), .after = battery_status) %>% 
  mutate(
    battery_status = case_when(
      n_bat == 1 ~ battery_status,  # Not changing status when the block was matched with only one battery status
      # all others will be cases of n_bat > 1
      lengths(all_battery_statuses[]) == 1 ~ as.character(all_battery_statuses),
      any('10-100%' %in% battery_status) ~ 'Partial Insufficient Battery',
      
      # last case has mix of '0-10%' and 'No Battery Data'
      T ~ 'Insufficient Battery'
    )
  ) %>% 
  filter(row_number() == 1) %>% 
  ungroup()

if(F){ema_dat_undelivered %>% count(battery_status, all_battery_statuses) %>% View}
if(F){ema_dat_undelivered %>% count(battery_status)}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2D: Update Battery Status for Participants Missing All Battery Data  ####
#   * This classification is a participant level and different from 'No Battery Data' which is a potential proxy for the phone being powered off
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ema_dat_undelivered <- ema_dat_undelivered %>% 
  group_by(ID_enrolled) %>% 
  mutate(battery_status = case_when(
    all(is.na(battery_status)) ~ 'Participant Level No Battery Data',
    T ~ battery_status
)) %>% ungroup()

if(F){ema_dat_undelivered %>% count(battery_status)}
if(F){ema_dat_undelivered %>% count(battery_status, after_pt_withdrew, no_day_start_on_study_day)}

# Remove extra columns
ema_dat_undelivered <- ema_dat_undelivered %>% select(-all_battery_statuses, -n_bat, -battery_hrts_local_start, -battery_hrts_local_end)

# End of Step 2
remove(battery_binned_cc1)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 3: Start Labeling Undelivered Reason using Hierarchy (from top down to where need conditions log) ####
# -o-  After Withdrew
# -o-  No Day Start 
# -o-  After End of Day (the end of day occurred before the expected start time of the block)
# -o-  No Battery Data (phone off / out of power); Insufficient Battery (entire block, source: Battery Data)
# --- 
# -x-  Conditions from phone log (driving, insufficient battery, bad quality, invalid block)
# -x-  End of Day During Block
# -x-  Unidentified Cause
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ema_dat_undelivered <- ema_dat_undelivered %>% 
  mutate(
    undelivered_rsn = case_when(
      after_pt_withdrew ~ "AFTER_WITHDREW_DATE",
      no_day_start_on_study_day ~ "NO_DAY_START",
      end_of_day_before_block_start ~ "AFTER_END_OF_DAY",
      battery_status == 'No Battery Data' ~ "NO_BATTERY_DATA",
      battery_status %in% c('0-10%', 'Insufficient Battery') ~ "INSUFFICIENT_BATTERY",
      # All else needs further reconciling, will bring in the phone status data for those
      T ~ NA_character_
    ), .after = block
  )

if(F){ema_dat_undelivered %>% count(undelivered_rsn) %>% mutate(percent = n/sum(n)*100)} # >80% undelivered EMAs are reconciled from the above

undelivered_reconciled <- ema_dat_undelivered %>% filter(!is.na(undelivered_rsn)) # Hold these until the others are reconciled. Will re-combine, update status variable, then re-combine with the delivered EMAs

undelivered_not_reconciled <- ema_dat_undelivered %>% filter(is.na(undelivered_rsn))  # These will be merged with the phone log data to identify the reason for undelivery 

remove(ema_dat_undelivered)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4: Join Phone Status Data with remaining unreconciled Undelivered EMAs ####
#     4A: Pre-process the phone status data for the join
#     4B: Execute the 'Between' Join
#     4C: Post-join Cleaning
#     4D: Identify the reason for undelivery when able to get that from the phone log
#     4E: Identify the reason for undelivery for the remaining EMAs
# 
#   NOTES:
#     * The phone status data has a discrete timestamp for each row, so the join will be matching the phone status data in a block's time interval
#       - To do this, we will use dplyr 1.1.0 join_by with 'between'
#           o https://dplyr.tidyverse.org/reference/join_by.html
#
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4A: Pre-process the phone status data for the join ####
#     * Data currently has unix time and a variable called 'current time'
#     * Need to convert unix to UTC and local time
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

crosswalk_id_enrolled <- dat_master %>% filter(cc_indicator == 1) %>% select(ID_enrolled, participant_id, timezone_local) %>% 
  add_row(
    dat_master %>% filter(cc_indicator == 1 & !is.na(participant_id_2)) %>% select(ID_enrolled, participant_id = participant_id_2, timezone_local)
  )

phone_status_cc1 <- phone_status_cc1 %>% 
  left_join( y = crosswalk_id_enrolled,
             by = 'participant_id' )

phone_status_cc1 <- phone_status_cc1 %>% 
  filter(!is.na(ID_enrolled)) %>%                                       # filter out participants who are not in dat_master, they were not enrolled
  relocate(ID_enrolled, timezone_local, .before = participant_id) %>% 
  select(-current_time)                                                 # remove the existing current time variable. appears to have the wrong timezone set

phone_status_cc1 <- phone_status_cc1 %>% 
  mutate(hrts_UTC = as_datetime(as.integer(unixtime)), .after = unixtime) %>% 
  mutate(hrts_local = map2(hrts_UTC, timezone_local,
                                        ~with_tz( .x, .y) %>% force_tz( ., "UTC") ) %>% unlist %>% as_datetime(.), .after = hrts_UTC )

phone_status_cc1 <- phone_status_cc1 %>% 
  mutate(ema_type = str_split(id, pattern = '_EMA', simplify = TRUE)[,1], .after = id) %>%  # Create variable from string id which indicates the EMA type
  select(-id)                                                                               # remove the id variable after creating ema_type from it


phone_status_cc1 <- phone_status_cc1 %>% 
  select(!contains("STRESS") & !contains("SMOKING")) %>%        # Remove columns not pertinent to Random EMAs
  filter(ema_type == 'RANDOM') %>%                              # Remove rows not pertinent to Random EMAs
  select(-study_day)                                            # Remove study day variable, we calculate one more robustly from dat_master

phone_status_cc1 <- phone_status_cc1 %>% rename_with(.fn = ~paste0('phone_log_', .x), .cols = 2:ncol(.))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4B: Execute the 'Between' Join ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

by <- join_by(ID_enrolled, between(x$phone_log_hrts_local, y$block_start_hrts_local, y$block_end_hrts_local)) # Dont need to join by ema_type because its only Random EMAs we're looking at

undelivered_not_reconciled <- right_join(x = phone_status_cc1,
                             y = undelivered_not_reconciled,
                             by)

remove(by)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4C: Post-join Cleaning ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

undelivered_not_reconciled <- undelivered_not_reconciled %>% 
  relocate(block_start_hrts_local, block_end_hrts_local, .after = phone_log_hrts_local) %>%
  relocate(phone_log_timezone_local, phone_log_participant_id, phone_log_hrts_UTC, phone_log_unixtime, .after = everything()) %>% 
  relocate(study_date, study_day_int, block, .after = ID_enrolled)

undelivered_not_reconciled <- undelivered_not_reconciled %>% arrange(ID_enrolled, study_day_int, block, phone_log_hrts_local)

if(F){undelivered_not_reconciled %>% group_by(ID_enrolled, study_day_int, block) %>% filter(any(phone_log_status == 'COMPLETED')) %>% View}

# The above view shows that the phone log recorded EMAs as completed despite them never being delivered to the phone.
# Similarly, the above view shows that the phone checked to send an EMA after but in the same block as one being 
# recorded as "Completed". These marked completed are erroneous

undelivered_not_reconciled <- undelivered_not_reconciled %>% select(-phone_log_VALID_BLOCK.VALID_BLOCK_RANDOM_EMA..CONDITION, )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4D: Identify the reason for undelivery when able to get that from the phone log ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

undelivered_not_reconciled <- undelivered_not_reconciled %>% 
  group_by(ID_enrolled, study_day_int, block) %>% 
  mutate(
    n_invalid_block = sum(phone_log_VALID_BLOCK.VALID_BLOCK_RANDOM_EMA..STATUS == 'false', na.rm = TRUE),
    n_poor_quality = sum(phone_log_DATA_QUALITY.DATA_QUALITY_5..STATUS == 'false', na.rm = TRUE),
    n_driving = sum(phone_log_NOT_DRIVING.NOT_DRIVING_5..STATUS == 'false', na.rm = TRUE),
    n_insuf_battery = sum(phone_log_PHONE_BATTERY.PHONE_BATTERY_10..STATUS == 'false', na.rm = TRUE),
    n_privacy_mode = sum(phone_log_PRIVACY.PRIVACY..STATUS == 'false', na.rm = TRUE),
    n_active = sum(phone_log_NOT_ACTIVE.NOT_ACTIVE_5..STATUS == 'false', na.rm = TRUE),
    .after = block ) %>% 
  filter(row_number() == 1) %>% 
  ungroup()

colnames_of_interest <- c('n_invalid_block', 'n_poor_quality', 'n_driving', 'n_insuf_battery', 'n_privacy_mode', 'n_active')

undelivered_not_reconciled <- undelivered_not_reconciled %>% 
  rowwise() %>% 
  mutate(
    cond_rsn_und = colnames_of_interest[which.max(c_across(all_of(colnames_of_interest)))], .after = block
) %>% 
  mutate(
    cond_rsn_und = ifelse( rowSums(across(all_of(colnames_of_interest))) == 0, NA_character_, cond_rsn_und )
    , .after = cond_rsn_und 
  ) %>% 
  relocate(undelivered_rsn, .after = block)

cond_crosswalk <- tribble(
  ~cond_rsn_und,        ~undelivered_rsn,
  'n_invalid_block',    'INVALID_BLOCK',   
  'n_poor_quality',     'POOR_DATA_QUALITY',
  'n_driving',          'DRIVING',
  'n_insuf_battery',    'INSUFFICIENT_BATTERY',
  'n_privacy_mode',     'PRIVACY_MODE',
  'n_active',           'ACTIVE'
)

undelivered_not_reconciled <- undelivered_not_reconciled %>% select(-undelivered_rsn)

undelivered_not_reconciled <- undelivered_not_reconciled %>% 
  left_join(y = cond_crosswalk,
            by = 'cond_rsn_und')

undelivered_not_reconciled <- undelivered_not_reconciled %>% relocate(undelivered_rsn, .after = block)

remove(cond_crosswalk)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4E: Identify the reason for undelivery for the remaining EMAs ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
undelivered_not_reconciled %>% count(undelivered_rsn)

if(F){undelivered_not_reconciled %>% filter(is.na(undelivered_rsn)) %>% count(end_of_day_during_block, battery_status)}


undelivered_not_reconciled <- undelivered_not_reconciled %>% 
  mutate(
    undelivered_rsn = case_when(
      !is.na(undelivered_rsn) ~ undelivered_rsn,  # only updating the rows without a value / reason assigned
      end_of_day_during_block ~ "END_OF_DAY_DURING_BLOCK",
      battery_status == 'Partial Insufficient Battery' ~ "INSUFFICIENT_BATTERY",
      T ~ "UNIDENTIFIED_CAUSE" ) )

# End of Step 4
remove(crosswalk_id_enrolled)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 5: Use undelivered reason to update the status variable ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

test_2 <- test_that(desc = "Check that we preserve the correct number of rows", {
  expect_equal(object = nrow(ema_dat_delivered) + nrow(undelivered_reconciled) + nrow(undelivered_not_reconciled),
               expected = nrow(block_level_ema_cc1))
})

undelivered_reconciled <- undelivered_reconciled %>% add_row(undelivered_not_reconciled %>% select(colnames(undelivered_reconciled)))

remove(undelivered_not_reconciled)


if(F){undelivered_reconciled %>% count(undelivered_rsn)}

test_3 <- test_that(desc = "Check that every undelivered EMA has an undelivered reason", {
  expect_equal(object = nrow(undelivered_reconciled %>% filter(is.na(undelivered_rsn))),
               expected = 0L)
})


undelivered_reconciled <- undelivered_reconciled %>% mutate(status = paste0( 'UNDELIVERED-' , undelivered_rsn ) )

if(F){undelivered_reconciled %>% count(status)}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 6: Update statuses for delivered EMAs ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

crosswalk_statuses <- tribble(
  ~original_status,          ~with_any_response,         ~updated_status,
  "COMPLETED",                1,                          "EMA-COMPLETE",
  "ABANDONED_BY_TIMEOUT",    1,                          "EMA-PARTIALLY_COMPLETE",
  "ABANDONED_BY_TIMEOUT",    0,                          "EMA-NO_RESPONSES",
  "ABANDONED_BY_USER",       0,                          "EMA-CANCELLED",
  "MISSED",                  0,                          "EMA-MISSED"
)

ema_dat_delivered <- ema_dat_delivered %>% rename(original_status = status)

ema_dat_delivered <- ema_dat_delivered %>% 
  left_join(y = crosswalk_statuses,
            by = c("original_status", "with_any_response"))

ema_dat_delivered <- ema_dat_delivered %>% 
  relocate(updated_status, .after = original_status) %>% 
  select(-original_status) %>% 
  rename(status = updated_status)

if(F){ema_dat_delivered %>% count(status)}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 7: Recombine undelivered and delivered data and save dataset ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

test_4 <- test_that(desc = "Check that we preserve the correct number of rows", {
  expect_equal(object = nrow(ema_dat_delivered) + nrow(undelivered_reconciled),
               expected = nrow(block_level_ema_cc1))
})

undelivered_rsn_block_level_ema_cc1 <- ema_dat_delivered %>% add_row(undelivered_reconciled %>% select(colnames(ema_dat_delivered)))

if(F){undelivered_rsn_block_level_ema_cc1 %>% count(status, ema_delivered)}


if(all(test_1, test_2, test_3, test_4)){
  print("Passed all tests. Saving data as block_level_ema_dataset_cc1.RDS")
  saveRDS(undelivered_rsn_block_level_ema_cc1,
          file = file.path(path_ontrack_ema_staged, "block_level_ema_dataset_cc1.RDS"))
} else {
  print("Did not pass tests")
}

# Bottom of page ####