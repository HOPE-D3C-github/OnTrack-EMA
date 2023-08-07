# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary ####
# Step 1: Build out backbone day-blocks based on first-day date
#
# Step 2: Process the day-start and day-end data to create a dataset with Day Start and Day Ends
#     - 2.1: Load and pre-process the Day Start and Day End data
#     - 2.2: Truncate Day Start and Day End for Participants with Two Phones
#     - 2.3: Join the Day Start and Day End data
#     - 2.4: Process Multiple Day-Starts
#
# Step 3: Integrate the backbone day-block data set with the Day-Start/End data set
#       - 3.1: Calculate block start times from day start data
#       - 3.2: Join the day start & end data with block start times data set to the backbone dates data set
#       - 3.3: Calculate theoretical block end time
#       - 3.4: Stack rows of the block level backbone for each EMA type
#
# Step 4: Integrate the EMA data with the backbone day-block data set
#       - 4.1: Re-process EMA timestamps to include a "local" time instead of "America/Chicago"
#       - 4.2: Truncate EMAs for PT with 2 Phones
#       - 4.3: Join EMA data to the block level backbone 
#       - 4.4: Remove EMAs that occured outside the study period 
#       - 4.5: Match the remaining unmatched EMAs to study day and block
#       - 4.6: Process unmatched day-blocks-ema_type
#       - 4.7: Combine the matched and unmatched datasets
#
# Step 5: Identify extra EMAs and EMAs delivered after the End of Day
#
# Step 6: Identify days of transitioning from phone 1 and 2
#
# Step 7. Output block-level EMA dataset ready for undelivered EMAs to be investigated
#
#
# Inputs:   file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData")
#           file.path(path_ontrack_ema_staged, "day_start_and_end_cc2.RData")
#           file.path(path_ontrack_visit_outputs, "masterlist.RData")
#           file.path(path_ontrack_ema_staged, "all_ema_data_cc1.RData")
#           file.path(path_ontrack_ema_staged, "all_ema_data_cc2.RData")
#
# Outputs:  file.path(path_ontrack_ema_staged, "block_level_ema_dataset_pre-undelivered_rsn.RData")
#           file.path(path_ontrack_ema_staged, "masterlist_updated.RDS")
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
library(dplyr)
library(tidyr)
library(data.table)
library(testthat)
library(lubridate)
library(purrr)

source("paths.R")
load(file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData"))
remove(all_random_ema_response_files_cc1, all_smoking_ema_response_files_cc1, all_stress_ema_response_files_cc1, all_log_phone_files_cc1, dat_file_counts_cc1)
load(file.path(path_ontrack_ema_staged, "day_start_and_end_cc2.RData"))
load(file = file.path(path_ontrack_visit_outputs, "masterlist.RData"))
load(file.path(path_ontrack_ema_staged, "all_ema_data_cc1.RData"))
load(file.path(path_ontrack_ema_staged, "all_ema_data_cc2.RData"))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1: Build out backbone day-blocks based on first-day date ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

dat_master_start_end_dates <- dat_master %>%
  select(ID_enrolled, participant_id, participant_id_2, cc_indicator, first_day_date, last_day_date, withdrew, withdrew_date)

dates_w_block_long <- data.frame(ID_enrolled = character(),
                                 participant_id = character(),
                                 participant_id_2 = character(),
                                 cc_indicator = character(),
                                 study_date = as.Date(character()),
                                 study_day_int = integer(),
                                 block = character(),
                                 withdrew_date = as.Date(character()))

for (i in 1:nrow(dat_master_start_end_dates)){
  dates_w_block_long <- dates_w_block_long %>% 
    add_row(expand_grid(ID_enrolled = dat_master_start_end_dates$ID_enrolled[i],
                        participant_id = dat_master_start_end_dates$participant_id[i],
                        participant_id_2 = dat_master_start_end_dates$participant_id_2[i],
                        cc_indicator = dat_master_start_end_dates$cc_indicator[i],
                        study_date = as_date(dat_master_start_end_dates$first_day_date[i] : dat_master_start_end_dates$last_day_date[i]),
                        block = as.character(1:4),
                        withdrew_date = dat_master_start_end_dates$withdrew_date[i]) %>%
              mutate(study_day_int = ceiling(row_number()/4), .after = study_date))
}

remove(dat_master_start_end_dates)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2: Process the day-start and day-end data to create a dataset with Day Start and Day Ends ####
# 
# Subtasks:
#     - Step 2.1: Load and pre-process the Day Start and Day End data
#     - Step 2.2: Truncate Day Start and Day End for Participants with Two Phones
#     - Step 2.3: Join the Day Start and Day End data
#     - Step 2.4: Process Multiple Day-Starts
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2.1: Load and pre-process the Day Start and Day End data ----
#
#   -Updated to match with ID_enrolled versus participant_id
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
all_day_start_files_cc2 <- all_day_start_files_cc2 %>% map(., ~rename_with(.x, ~colnames(all_day_start_files_cc1[[1]])))  # CC2 used V1, V2, V3 instead of X1, X2, X3 as column names

day_start <- rbindlist(c(all_day_start_files_cc1, all_day_start_files_cc2))

day_start <- day_start %>% 
  left_join(y = dat_master %>% select(ID_enrolled, participant_id, timezone_local),
            by = c("participant_id")) %>%
  left_join(y = dat_master %>% select(ID_enrolled, participant_id_2, timezone_local),
            by = c("participant_id" = "participant_id_2"))

day_start <- day_start %>% add_row(day_start %>% filter(participant_id == 'ses_119') %>% select(-ID_enrolled.x)) # ses_118 had data stored under ses_119 as well


day_start <- day_start %>% 
  mutate(ID_enrolled = coalesce(ID_enrolled.x, ID_enrolled.y),
         timezone_local = coalesce(timezone_local.x, timezone_local.y)) %>% 
  select(-c(ID_enrolled.x, ID_enrolled.y, timezone_local.x, timezone_local.y)) %>% 
  filter(!is.na(ID_enrolled)) %>% 
  relocate(ID_enrolled, .before = everything())

day_start <- day_start %>% 
  mutate(
    X1hrts_local = map2(as.POSIXct(as.numeric(X1/1000), tz = "UTC", origin="1970-01-01"), timezone_local,
                        ~with_tz(.x, .y) %>% force_tz(., "UTC")) %>% unlist %>% as_datetime(.), 
    X3hrts_local = map2(as.POSIXct(as.numeric(X3/1000), tz = "UTC", origin="1970-01-01"), timezone_local,
                        ~with_tz(.x, .y) %>% force_tz(., "UTC")) %>% unlist %>% as_datetime(.)
  )

day_end <- rbindlist(c(all_day_end_files_cc1, all_day_end_files_cc2), fill = TRUE) %>% 
  mutate(X1 = coalesce(X1, V1),
         X2 = coalesce(X2, V2),
         X3 = coalesce(X3, V3)) %>% 
  select(-c(V1, V2, V3))

day_end <- day_end %>% 
  left_join(y = dat_master %>% select(ID_enrolled, participant_id, timezone_local), 
            by = c("participant_id")) %>% 
  left_join(y = dat_master %>% select(ID_enrolled, participant_id_2, timezone_local),
            by = c("participant_id" = "participant_id_2"))

day_end <- day_end %>% add_row(day_end %>% filter(participant_id == 'ses_119') %>% select(-ID_enrolled.x)) # ses_118 had data stored under ses_119 as well

day_end <- day_end %>% 
  mutate(ID_enrolled = coalesce(ID_enrolled.x, ID_enrolled.y),
         timezone_local = coalesce(timezone_local.x, timezone_local.y)) %>% 
  select(-c(ID_enrolled.x, ID_enrolled.y, timezone_local.x, timezone_local.y)) %>% 
  filter(!is.na(ID_enrolled)) %>% 
  relocate(ID_enrolled, .before = everything())

day_end <- day_end %>% 
  mutate(X1hrts_local = map2(as.POSIXct(as.numeric(X1/1000), tz = "UTC", origin="1970-01-01"), timezone_local,
                             ~with_tz(.x, .y) %>% force_tz(., "UTC")) %>% unlist %>% as_datetime(.), 
         X3hrts_local = map2(as.POSIXct(as.numeric(X3/1000), tz = "UTC", origin="1970-01-01"), timezone_local,
                             ~with_tz(.x, .y) %>% force_tz(., "UTC")) %>% unlist %>% as_datetime(.))

test1 <- test_that(desc = "The two timestamp values per row are within 1 second of eachother", {
  for (i in 1:length(day_start)){
    expect_lt(
      abs(day_start$X1hrts_local[i] - day_start$X3hrts_local[i]), as.difftime(1, units = "secs"))
  }
  for (i in 1:length(day_end)){
    expect_lt(
      abs(day_end$X1hrts_local[i] - day_end$X3hrts_local[i]), as.difftime(1, units = "secs"))
  }
})

test2 <- test_that(desc = "No missing timestamps",{
  expect_true(any(is.na(day_start$X1hrts_local)) == FALSE)
  expect_true(any(is.na(day_end$X1hrts_local)) == FALSE)
})

if(test1 & test2){
  day_start <- day_start %>% 
    select(ID_enrolled, participant_id, X1, timezone_local, X1hrts_local) %>% 
    rename(day_start_unixts = X1, day_start_hrts_local = X1hrts_local) %>% 
    arrange(ID_enrolled, day_start_unixts) %>%
    group_by(ID_enrolled) %>% 
    mutate(lead_day_start_hrts_local = lead(day_start_hrts_local, 
                                            default = as.POSIXct(as.numeric("1996356467"), tz = "UTC", origin="1970-01-01") # default required as a filler to have a value for the last day_start record
                                            ),
           .after = day_start_hrts_local) %>% 
    ungroup()
  
  day_start <- day_start %>% filter(!(lead_day_start_hrts_local - day_start_hrts_local < minutes(5)))
  
  day_end <- day_end %>% 
    select(ID_enrolled, participant_id, X1, timezone_local, X1hrts_local) %>% 
    rename(day_end_unixts = X1, day_end_hrts_local = X1hrts_local) %>% 
    arrange(ID_enrolled, day_end_unixts) %>%
    group_by(ID_enrolled) %>% 
    mutate(lead_day_end_hrts_local = lead(day_end_hrts_local, 
                                    default = as.POSIXct(as.numeric("1996356467"), tz = "UTC", origin="1970-01-01") %>% with_tz(., "America/Chicago")),
           .after = day_end_hrts_local) %>% 
    ungroup()
  
  day_end <- day_end %>% filter(!(lead_day_end_hrts_local - day_end_hrts_local < minutes(5)))
}

day_start <- day_start %>% mutate(start_date = as_date(day_start_hrts_local))

# Looking at days where a participant has multiple day_start records - Already removed duplicates (within 5 minutes of another)
# known software issue - we saw this in Break Free and it caused extra EMAs to be sent out
multiple_day_start_same_day <- day_start %>% filter(lag(start_date) == start_date | lead(start_date) == start_date)

# The most extra day_starts per day is 2
if(F){day_start %>% arrange(ID_enrolled, day_start_unixts) %>% 
  group_by(ID_enrolled, start_date) %>% mutate(n = n()) %>% ungroup() %>% select(n) %>% summary()}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2.2: Truncate Day Start and Day End for Participants with Two Phones ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dat_master_2_phones <- dat_master %>% filter(!is.na(participant_id_2) & participant_id != 'ses_118')

day_start_2_phones <- day_start %>% filter(ID_enrolled %in% dat_master_2_phones$ID_enrolled) %>% 
  left_join(y = dat_master_2_phones %>% select(ID_enrolled, first_day_date, last_day_date, phone_1 = participant_id, phone_2 = participant_id_2),
            by = "ID_enrolled") %>% 
  mutate(phone_int = case_when(participant_id == phone_1 ~ "1",
                               participant_id == phone_2 ~ "2",
                               T ~ NA_character_), .after = participant_id) %>% 
  filter(dplyr::between(start_date, first_day_date, last_day_date)) %>%  
  select(-c(first_day_date, last_day_date, phone_1, phone_2)) %>% 
  mutate(transition_date = case_when(
    n() == 1 ~ FALSE,
    phone_int[1] != phone_int[2] ~ TRUE,
    T ~ NA), .by = c(ID_enrolled, start_date))

first_day_start_2nd_phone <- day_start_2_phones %>% 
  filter(transition_date & phone_int == 2) %>% 
  filter(row_number() == 1, .by = ID_enrolled)

dat_master <- dat_master %>% 
  left_join(y = first_day_start_2nd_phone %>% select(ID_enrolled, start_phone_2_unixts = day_start_unixts, start_phone_2_hrts_local = day_start_hrts_local),
            by = "ID_enrolled") %>% 
  mutate(has_2_phones = !is.na(start_phone_2_unixts))

day_start <- day_start %>% 
  left_join(dat_master %>% select(ID_enrolled, phone_1 = participant_id, phone_2 = participant_id_2, start_phone_2_hrts_local),
            by = 'ID_enrolled') %>% 
  mutate(phone_int = case_when(participant_id == phone_1 ~ "1",
                               participant_id == phone_2 ~ "2",
                               T ~ NA_character_), .after = participant_id) %>% 
  filter((phone_int == 1 & (day_start_hrts_local < start_phone_2_hrts_local | is.na(start_phone_2_hrts_local))) | phone_int == 2) # only keep phone 1 data from before the start of phone 2

day_start <- day_start %>% select(-c(phone_1, phone_2))

day_end <- day_end %>% 
  left_join(dat_master %>% select(ID_enrolled, phone_1 = participant_id, phone_2 = participant_id_2, start_phone_2_hrts_local),
            by = 'ID_enrolled') %>% 
  mutate(phone_int = case_when(participant_id == phone_1 ~ "1",
                               participant_id == phone_2 ~ "2",
                               T ~ NA_character_), .after = participant_id) %>% 
  filter((phone_int == 1 & (day_end_hrts_local < start_phone_2_hrts_local | is.na(start_phone_2_hrts_local))) | phone_int == 2) # only keep phone 1 data from before the start of phone 2

day_end <- day_end %>% select(-c(phone_1, phone_2))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2.3: Join the Day Start and Day End data ----
# 
# foverlaps works but requires re-joining 
# day starts that had no day-end data
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

day_start <- data.table(day_start)
day_end <- data.table(day_end %>% mutate(day_end_hrts_local_2 = day_end_hrts_local))

setkey(day_start, "ID_enrolled", "participant_id", "phone_int", "start_phone_2_hrts_local", "timezone_local", "day_start_hrts_local", "lead_day_start_hrts_local")

interval_join_daystart_dayend <- foverlaps(
  day_end,
  day_start,
  by.x = c("ID_enrolled", "participant_id", "phone_int", "start_phone_2_hrts_local", "timezone_local", "day_end_hrts_local", "day_end_hrts_local_2"),
  type = "within"
  )

# Remove rows where any additional day-end is matched to the same day-start. Keep the last day-end, since the earlier day-end didnt appear to be processed correctly
# Only occurs once - participant_id: 125881_R63; day_start_hrts_local: 2018-01-18 07:44:46; day_end_hrts_local: 2018-01-19 06:22:32
interval_join_daystart_dayend <- interval_join_daystart_dayend %>% group_by(ID_enrolled, day_start_unixts) %>% filter(row_number() == n()) %>% ungroup()

interval_join_daystart_dayend <- interval_join_daystart_dayend %>% filter(!is.na(day_start_unixts))

# Need to add back in the day start data for days without day-ends 
day_start_wo_day_end <- day_start %>% 
  anti_join(y = interval_join_daystart_dayend,
            by = c("ID_enrolled", "participant_id", "day_start_unixts"))

test3 <- test_that(
  desc = "Testing that the number of rows being added to the interval join by the day_start_wo_day_end equals to total number of day starts",
  code = {
    expect_equal(
      object = nrow(day_start_wo_day_end) + nrow(interval_join_daystart_dayend),
      expected = nrow(day_start))}
  )

if(test3){
  day_start_and_end <- interval_join_daystart_dayend %>% 
    add_row(day_start_wo_day_end) %>% 
    arrange(participant_id, day_start_unixts)
}

day_start_and_end <- day_start_and_end %>% select(-c(lead_day_start_hrts_local, lead_day_end_hrts_local, day_end_hrts_local_2, phone_int))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2.4: Process Multiple Day-Starts ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

multdaystart_day_start_and_end <- day_start_and_end %>% group_by(ID_enrolled, start_date) %>% filter(n()>1) %>% ungroup()
singledaystart_day_start_and_end <- day_start_and_end %>% group_by(ID_enrolled, start_date) %>% filter(n()==1) %>% ungroup()


test4 <- test_that("Split preserves the original number of rows",{
  expect_equal(object = nrow(multdaystart_day_start_and_end) + nrow(singledaystart_day_start_and_end),
               expected = nrow(day_start_and_end))
})

if(test4){
  updated_multdaystart <- multdaystart_day_start_and_end %>% 
    arrange(ID_enrolled, day_start_unixts) %>%
    group_by(ID_enrolled, start_date) %>% 
    mutate(day_start_unixts_2 = day_start_unixts[n()],
           day_start_hrts_local_2 = day_start_hrts_local[n()],
           day_end_unixts_2 = day_end_unixts[n()],
           day_end_hrts_local_2 = day_end_hrts_local[n()]) %>% 
    filter(row_number()==1) %>% 
    ungroup()

  day_start_and_end <- updated_multdaystart %>% 
    add_row(singledaystart_day_start_and_end) %>% 
    arrange(ID_enrolled, day_start_unixts) %>% 
    mutate(extra_day_start = !is.na(day_start_unixts_2))
}

day_start_and_end <- day_start_and_end %>% 
  arrange(ID_enrolled, day_start_unixts) %>% 
  group_by(ID_enrolled) %>% 
  mutate(lead_day_start_hrts_local = lead(day_start_hrts_local)) 

# End of Step 2
remove(
   all_day_start_files_cc1, all_day_end_files_cc1, all_day_start_files_cc2, all_day_end_files_cc2,
   day_start, day_end, multiple_day_start_same_day, interval_join_daystart_dayend, day_start_wo_day_end,
   multdaystart_day_start_and_end, singledaystart_day_start_and_end, updated_multdaystart
  )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 3: Integrate the backbone day-block data set with the Day-Start/End data set ####
# 
#   Subtasks:
#       - Step 3.1: Calculate block start times from day start data
#       - Step 3.2: Join the day start & end data with block start times data set to the backbone dates data set
#       - Step 3.3: Calculate theoretical block end time
#       - Step 3.4: Stack rows of the block level backbone for each EMA type
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 3.1: Calculate block start times from day start data ----
#
# NOTE: theoretical start times; not accounting for early end-day
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
day_start_and_end_w_blocks_wide <- day_start_and_end %>% 
  rename(study_date = start_date) %>% 
  mutate(block_1_start_hrts_local = day_start_hrts_local,
         block_2_start_hrts_local = day_start_hrts_local + hours(4),
         block_3_start_hrts_local = day_start_hrts_local + hours(8),
         block_4_start_hrts_local = day_start_hrts_local + hours(12)
         )

day_start_and_end_w_blocks_tall <- day_start_and_end_w_blocks_wide %>% 
  pivot_longer(
    cols = starts_with("block_"),
    names_to = "block",
    names_prefix = "block_", 
    values_to = "block_start_hrts_local"
  ) %>% 
  mutate(block = str_split_fixed(block, "_", n =2)[,1])

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 3.2: Join the day start & end data with block start times data set to the backbone dates data set ----
#
# left join (x = dates_w_block_long, y = day_start_and_end_w_blocks_tall) because dates_w_block_long is 
#   built out from the curated EMA period dates, whereas day_start_and_end_w_blocks_tall is looking at all
#   data collected
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

block_level_backbone <- dates_w_block_long %>% select(-participant_id, -participant_id_2) %>% 
  left_join(y = day_start_and_end_w_blocks_tall,
            by=c("ID_enrolled", "study_date", "block")) 

test5 <- test_that("Check that no rows are added",{
  expect_equal(object = nrow(block_level_backbone),
               expected = nrow(dates_w_block_long))
})

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 3.3: Calculate theoretical block end time ----
# 
#     - end time of block 4 on a day with an extra day start:
#           [time of second day start] + hours(16)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
block_level_backbone <- block_level_backbone %>% 
  arrange(ID_enrolled, study_date, block) %>% 
  #rowwise() %>% 
  #group_by(ID_enrolled) %>% 
  mutate(block_end_hrts_local = case_when(
    !(block == "4" & extra_day_start) & is.na(lead_day_start_hrts_local) ~ block_start_hrts_local + hours(4),
    !(block == "4" & extra_day_start) & block_start_hrts_local + hours(4) < lead_day_start_hrts_local ~ block_start_hrts_local + hours(4),
    !(block == "4" & extra_day_start) & block_start_hrts_local + hours(4) > lead_day_start_hrts_local & block_start_hrts_local > lead_day_start_hrts_local ~ block_start_hrts_local,
    !(block == "4" & extra_day_start) & block_start_hrts_local + hours(4) > lead_day_start_hrts_local & block_start_hrts_local < lead_day_start_hrts_local ~ lead_day_start_hrts_local,
    # block 4 and extra day start for all cases that make it below here
    is.na(lead_day_start_hrts_local) ~ day_start_hrts_local_2 + hours(16),
    day_start_hrts_local_2 + hours(16) < lead_day_start_hrts_local ~ day_start_hrts_local_2 + hours(16),
    day_start_hrts_local_2 + hours(16) > lead_day_start_hrts_local & block_start_hrts_local > lead_day_start_hrts_local ~ block_start_hrts_local,
    day_start_hrts_local_2 + hours(16) > lead_day_start_hrts_local & block_start_hrts_local < lead_day_start_hrts_local ~ lead_day_start_hrts_local
  )) %>% ungroup()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 3.4: Updating Block Times for Days of Transitioning Phones ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# block_level_backbone_test <- block_level_backbone %>% 
#   mutate(start_phone_2_date = as_date(start_phone_2_hrts_local),
#          day_of_transitioning_phone = replace_na(study_date == start_phone_2_date, FALSE),
#          block_on_or_after_transitioning_phone = day_of_transitioning_phone & block_end_hrts_local > start_phone_2_hrts_local,
#          .after = start_phone_2_hrts_local
#          ) %>% 
#   mutate(
#     block_after_phone_2 = case_when(
#       !block_on_or_after_transitioning_phone ~ NA_integer_,
#       T ~ as.integer(block) - as.integer(block[1]) + 1
#     ), .by = c(ID_enrolled, study_day_int, block_on_or_after_transitioning_phone)
#   )
# 
# block_level_backbone_test %>% count(block_after_phone_2)

updated_for_start_of_phone_2 <- block_level_backbone %>% 
  filter(
    (ID_enrolled == '4070' & study_day_int == 5 & block %in% c("2", "3", "4")) |
    (
      ID_enrolled == '4112' & study_day_int == 8 
      ) |
    (
      ID_enrolled == '4127' & study_day_int == 8 & block %in% c("3", "4")
    ) |
    (
      ID_enrolled == '4136' & study_day_int == 8 
    ) |
    (
      ID_enrolled == '4165' & study_day_int == 6 & block %in% c("2", "3", "4")
    ) |
    (
      ID_enrolled == '4177' & study_day_int == 13
    ) |
    (
      ID_enrolled == '4179' & study_day_int == 8 & block %in% c("2", "3", "4")
    ) |
    (
      ID_enrolled == '4181' & study_day_int == 8 & block %in% c("3", "4")
    ) |
    (
      ID_enrolled == '4183' & study_day_int == 5 & block %in% c("3", "4")
    ))

not_updated_for_start_of_phone_2 <- block_level_backbone %>% anti_join(y = updated_for_start_of_phone_2)

nrow(updated_for_start_of_phone_2) + nrow(not_updated_for_start_of_phone_2) == nrow(block_level_backbone)

updated_for_start_of_phone_2 <- updated_for_start_of_phone_2 %>% 
  mutate(block_number_updated_per_pt = row_number(), .by = ID_enrolled) %>% 
  mutate(
    block_start_hrts_local = case_when(
      block_number_updated_per_pt == 1 ~ start_phone_2_hrts_local,
      T ~ start_phone_2_hrts_local + hours(4 * (block_number_updated_per_pt - 1))
    ), .after = block_start_hrts_local
  ) %>% 
  mutate(
    block_end_hrts_local = start_phone_2_hrts_local + hours(4 * (block_number_updated_per_pt)), .after = block_end_hrts_local
  ) %>% 
  select(all_of(colnames(block_level_backbone)))

block_level_backbone <- bind_rows(not_updated_for_start_of_phone_2, updated_for_start_of_phone_2) %>% arrange(ID_enrolled, study_day_int, block)

# Update phone 1 block_end to truncate early for phone_2_start
block_level_backbone <- block_level_backbone %>% 
  mutate(block_end_hrts_local = case_when(
    between(start_phone_2_hrts_local, lower = block_start_hrts_local, upper = block_end_hrts_local, incbounds = FALSE) ~ start_phone_2_hrts_local,
    T ~ block_end_hrts_local
  ))

# Update the indicator tracking multiple day starts for the days where participants switched from phone 1 to phone 2
block_level_backbone <- block_level_backbone %>% 
  mutate(
    extra_day_start = case_when(
      is.na(start_phone_2_hrts_local) ~ extra_day_start,
      as_date(start_phone_2_hrts_local) == study_date ~ FALSE,
      T ~ extra_day_start
    )
  )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 3.4: Stack rows of the block level backbone for each EMA type ----
#
#     - one row per participant, study day, block, EMA type
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

block_level_backbone <- block_level_backbone %>% mutate(ema_type = "RANDOM", .after = block) %>% 
  add_row(block_level_backbone %>% mutate(ema_type = "SMOKING", .after = block)) %>% 
  add_row(block_level_backbone %>% mutate(ema_type = "STRESS", .after = block)) %>% 
  arrange(ID_enrolled, study_date, block) %>% 
  select(-participant_id)

# End of Step 3
remove(
  dates_w_block_long, day_start_and_end_w_blocks_wide, day_start_and_end, day_start_and_end_w_blocks_tall
)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4: Integrate the EMA data with the backbone day-block data set ####
#
#   Subtasks:
#       - Step 4.1: Re-process EMA timestamps to include a "local" time instead of "America/Chicago"
#       - Step 4.2: Truncate EMAs for PT with 2 Phones
#       - Step 4.3: Join EMA data to the block level backbone 
#       - Step 4.4: Remove EMAs that occured outside the study period 
#       - Step 4.5: Process unmatched day-blocks-ema_type
#       - Step 4.6: Combine the matched and unmatched datasets
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4.1: Re-process EMA timestamps to include a "local" time instead of "America/Chicago" ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# CC1
all_ema_data_cc1 <- all_ema_data_cc1 %>% 
  left_join(y = dat_master %>% select(ID_enrolled, participant_id, timezone_local),
            by = c("participant_id")) %>% 
  left_join(y = dat_master %>% select(ID_enrolled, participant_id_2, timezone_local),
            by = c("participant_id" = "participant_id_2")) %>% 
  relocate(ID_enrolled.x, ID_enrolled.y, timezone_local.x, timezone_local.y, .after = participant_id)

all_ema_data_cc1 <- all_ema_data_cc1 %>% 
  mutate(
    ID_enrolled = coalesce(ID_enrolled.x, ID_enrolled.y), 
    .before = everything()
  ) %>% 
  mutate(
    timezone_local = coalesce(timezone_local.x, timezone_local.y), 
    .after = participant_id
  ) %>% 
  select(-c(ID_enrolled.x, ID_enrolled.y, timezone_local.x, timezone_local.y)) %>% 
  filter(!is.na(timezone_local))  #removes participants who were not enrolled

all_ema_data_cc1 <- all_ema_data_cc1 %>% 
  mutate(
    begin_hrts_local = map2(begin_hrts_UTC, timezone_local,
         ~with_tz(.x, .y) %>% force_tz(., "UTC")) %>% unlist %>% as_datetime(.),
    end_hrts_local = map2(end_hrts_UTC, timezone_local,
                          ~with_tz(.x, .y) %>% force_tz(., "UTC")) %>% unlist %>% as_datetime(.),
    .after = with_any_response
  ) %>% 
  select(-c(begin_hrts_AmericaChicago, end_hrts_AmericaChicago))


# CC2
all_ema_data_cc2 <- all_ema_data_cc2 %>% 
  left_join(y = dat_master %>% select(ID_enrolled, participant_id, timezone_local),
            by = c("participant_id")) %>% 
  left_join(y = dat_master %>% select(ID_enrolled, participant_id_2, timezone_local),
            by = c("participant_id" = "participant_id_2")) %>% 
  relocate(ID_enrolled.x, ID_enrolled.y, timezone_local.x, timezone_local.y, .after = participant_id)

all_ema_data_cc2 <- all_ema_data_cc2 %>% 
  mutate(
    ID_enrolled = coalesce(ID_enrolled.x, ID_enrolled.y), 
    .before = everything()
  ) %>% 
  mutate(
    timezone_local = coalesce(timezone_local.x, timezone_local.y), 
    .after = participant_id
  ) %>% 
  select(-c(ID_enrolled.x, ID_enrolled.y, timezone_local.x, timezone_local.y)) %>% 
  filter(!is.na(timezone_local))  #removes participants who were not enrolled

all_ema_data_cc2 <- all_ema_data_cc2 %>% 
  mutate(
    begin_hrts_local = map2(begin_hrts_UTC, timezone_local,
                            ~with_tz(.x, .y) %>% force_tz(., "UTC")) %>% unlist %>% as_datetime(.),
    end_hrts_local = map2(end_hrts_UTC, timezone_local,
                          ~with_tz(.x, .y) %>% force_tz(., "UTC")) %>% unlist %>% as_datetime(.),
    .after = with_any_response
  ) %>% 
  select(-c(begin_hrts_AmericaChicago, end_hrts_AmericaChicago))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4.2: Truncate EMAs for PT with 2 Phones ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
all_ema_data_cc1<- all_ema_data_cc1 %>% 
  left_join(dat_master %>% select(ID_enrolled, phone_1 = participant_id, phone_2 = participant_id_2, start_phone_2_hrts_local),
                                  by = 'ID_enrolled') %>% 
  mutate(phone_int_ema = case_when(participant_id == phone_1 ~ "1",
                               participant_id == phone_2 ~ "2",
                               T ~ NA_character_), .after = participant_id) %>%
  filter((phone_int_ema == 1 & (end_hrts_local < start_phone_2_hrts_local | is.na(start_phone_2_hrts_local))) | phone_int_ema == 2) %>% 
  select(-c(phone_1, phone_2, start_phone_2_hrts_local))
  
all_ema_data_cc2 <- all_ema_data_cc2 %>% 
  left_join(dat_master %>% select(ID_enrolled, phone_1 = participant_id, phone_2 = participant_id_2, start_phone_2_hrts_local),
            by = 'ID_enrolled') %>% 
  mutate(phone_int_ema = case_when(participant_id == phone_1 ~ "1",
                               participant_id == phone_2 ~ "2",
                               T ~ NA_character_), .after = participant_id) %>%
  filter((phone_int_ema == 1 & (end_hrts_local < start_phone_2_hrts_local | is.na(start_phone_2_hrts_local))) | phone_int_ema == 2) %>% 
  select(-c(phone_1, phone_2, start_phone_2_hrts_local))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4.3: Join EMA data to the block level backbone ----
#
#     - Interval join by the start and end time of the block, 
#       matching on ID_enrolled, participant_id and ema_type
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

block_level_backbone_wdaystart <- data.table(block_level_backbone %>% filter(!is.na(block_start_hrts_local)))

# CC1
all_ema_data_cc1 <- data.table(all_ema_data_cc1 %>% 
                                 mutate(
                                   ema_delivered_hrts_local = coalesce(begin_hrts_local, end_hrts_local),
                                   ema_delivered_hrts_local_2 = ema_delivered_hrts_local
                                   ))

setkey(block_level_backbone_wdaystart, "ID_enrolled", "ema_type", "block_start_hrts_local", "block_end_hrts_local")

interval_join_block_level_ema_cc1 <- foverlaps(
  all_ema_data_cc1,
  block_level_backbone_wdaystart,
  by.x = c("ID_enrolled", "ema_type", "ema_delivered_hrts_local", "ema_delivered_hrts_local_2"),
  type = "within"
)

test6 <- test_that("Testing CC1. No new rows added. Expect 1 row per row from EMA dataset", {
  expect_equal(object = nrow(interval_join_block_level_ema_cc1),
               expected = nrow(all_ema_data_cc1))
})

# CC2
all_ema_data_cc2 <- data.table(all_ema_data_cc2 %>% 
                                 mutate(
                                   ema_delivered_hrts_local = coalesce(begin_hrts_local, end_hrts_local),
                                   ema_delivered_hrts_local_2 = ema_delivered_hrts_local
                                 ))

setkey(block_level_backbone_wdaystart, "ID_enrolled", "ema_type", "block_start_hrts_local", "block_end_hrts_local")

interval_join_block_level_ema_cc2 <- foverlaps(
  all_ema_data_cc2,
  block_level_backbone_wdaystart,
  by.x = c("ID_enrolled", "ema_type", "ema_delivered_hrts_local", "ema_delivered_hrts_local_2"),
  type = "within"
)

test7 <- test_that("Testing CC2. No new rows added. Expect 1 row per row from EMA dataset", {
  expect_equal(object = nrow(interval_join_block_level_ema_cc2),
               expected = nrow(all_ema_data_cc2))
})

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4.4: Remove EMAs that occured outside the study period ----
# 
# Remove EMAs that were not matched to a row from the backbone block dataset
#  IF and only IF the datetime was outside the curated EMA period for that
#  participant
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# CC1
interval_join_block_level_ema_cc1 <- interval_join_block_level_ema_cc1 %>% 
  left_join(dat_master %>% select(ID_enrolled, first_day_date, last_day_date),
            by = c("ID_enrolled")) %>% 
  mutate(delivered_before_ema_period = as_date(ema_delivered_hrts_local) < first_day_date,
         delivered_after_ema_period = as_date(ema_delivered_hrts_local) > last_day_date,
         delivered_outside_ema_period = delivered_before_ema_period | delivered_after_ema_period
  ) 

interval_join_block_level_ema_cc1 <- interval_join_block_level_ema_cc1 %>% 
  filter((!is.na(study_date) | (is.na(study_date) & !delivered_outside_ema_period))) %>% 
  filter(!(is.na(study_date) & ID_enrolled == "4039")) # Special case, the ema's were delivered after midnight but related to the study day before the first day date


# CC2
interval_join_block_level_ema_cc2 <- interval_join_block_level_ema_cc2 %>% 
  left_join(dat_master %>% select(ID_enrolled, first_day_date, last_day_date),
            by = c("ID_enrolled")) %>% 
  mutate(delivered_before_ema_period = as_date(ema_delivered_hrts_local) < first_day_date,
         delivered_after_ema_period = as_date(ema_delivered_hrts_local) > last_day_date,
         delivered_outside_ema_period = delivered_before_ema_period | delivered_after_ema_period
  ) 

interval_join_block_level_ema_cc2 <- interval_join_block_level_ema_cc2 %>% 
  filter((!is.na(study_date) | (is.na(study_date) & !delivered_outside_ema_period)))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4.5: Match the remaining unmatched EMAs to study day and block
#
#   - Unmatched are due to being delivered outside the study day interval (after block 4)
#      but within the study period
#
# Updated 2/16/2023 after the dplyr update for interval joins
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

extended_block_4_backbone <- block_level_backbone_wdaystart %>% 
  mutate(block_end_hrts_local = as_datetime(ifelse(block == 4, lead(day_start_hrts_local), block_end_hrts_local)))

# CC1
matched_cc1 <- interval_join_block_level_ema_cc1 %>% filter(!is.na(study_date))
unmatched_cc1 <- interval_join_block_level_ema_cc1 %>% filter(is.na(study_date))

unmatched_cc1_postjoin <- unmatched_cc1 %>% select(-all_of(colnames(extended_block_4_backbone)[c(2:5, 7:length(colnames(extended_block_4_backbone)))])) %>% 
  left_join( y = extended_block_4_backbone,
             by = join_by(ID_enrolled, ema_type, between(x$ema_delivered_hrts_local, y$block_start_hrts_local, y$block_end_hrts_local)))

interval_join_block_level_ema_cc1 <- matched_cc1 %>% add_row(unmatched_cc1_postjoin) %>% arrange(ID_enrolled, study_day_int, block, end_hrts_local)

# CC2 
matched_cc2 <- interval_join_block_level_ema_cc2 %>% filter(!is.na(study_date))
unmatched_cc2 <- interval_join_block_level_ema_cc2 %>% filter(is.na(study_date))

unmatched_cc2_postjoin <- unmatched_cc2 %>% select(-all_of(colnames(extended_block_4_backbone)[c(2:5, 7:length(colnames(extended_block_4_backbone)))])) %>% 
  left_join( y = extended_block_4_backbone,
            by = join_by(ID_enrolled, ema_type, between(x$ema_delivered_hrts_local, y$block_start_hrts_local, y$block_end_hrts_local)))

interval_join_block_level_ema_cc2 <- matched_cc2 %>% add_row(unmatched_cc2_postjoin) %>% arrange(ID_enrolled, study_day_int, block, end_hrts_local)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4.6: Process unmatched day-blocks-ema_type
#
#   - Unmatched SMOKING and STRESS day-block-ema_type will be removed
#   - Unmatched RANDOM day-block-ema_type are considered "Undelivered"
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# CC1
unmatched_block_level_backbone_cc1 <- block_level_backbone %>% filter(cc_indicator == '1') %>% 
  anti_join(interval_join_block_level_ema_cc1,
            by = c("ID_enrolled", "study_date", "block", "ema_type"))

unmatched_block_level_backbone_cc1 <- unmatched_block_level_backbone_cc1 %>% 
  filter(ema_type == "RANDOM") %>% 
  mutate(status = "UNDELIVERED")

# CC2
unmatched_block_level_backbone_cc2 <- block_level_backbone %>% filter(cc_indicator == '2') %>% 
  anti_join(interval_join_block_level_ema_cc2,
            by = c("ID_enrolled", "study_date", "block", "ema_type"))

unmatched_block_level_backbone_cc2 <- unmatched_block_level_backbone_cc2 %>% 
  filter(ema_type == "RANDOM") %>% 
  mutate(status = "UNDELIVERED")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4.7: Combine the matched and unmatched datasets ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# CC1
block_level_ema_cc1 <- interval_join_block_level_ema_cc1 %>% 
  add_row(unmatched_block_level_backbone_cc1)

# CC2
block_level_ema_cc2 <- interval_join_block_level_ema_cc2 %>% 
  add_row(unmatched_block_level_backbone_cc2)

# End of Step 4
remove(
  all_ema_data_cc1, all_ema_data_cc2, 
  block_level_backbone, block_level_backbone_wdaystart, 
  interval_join_block_level_ema_cc1, interval_join_block_level_ema_cc2, 
  unmatched_block_level_backbone_cc1, unmatched_block_level_backbone_cc2
)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 5: Identify extra EMAs and EMAs delivered after the End of Day ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# CC1
block_level_ema_cc1 <- block_level_ema_cc1 %>% 
  arrange(ID_enrolled, study_date, block, begin_hrts_local) %>% 
  group_by(ID_enrolled, study_date, block, ema_type) %>% 
  mutate(extra_ema = row_number() > 1,
         extra_ema_on_block = n() > 1) %>% 
  ungroup() %>% 
  group_by(ID_enrolled, study_date) %>% 
  mutate(extra_ema_on_study_day = any(extra_ema)) %>% 
  ungroup()

block_level_ema_cc1 <- block_level_ema_cc1 %>% 
  mutate(invalid_end_day = case_when(
    is.na(end_hrts_local) ~ FALSE, # undelivered EMA
    is.na(day_end_hrts_local) ~ FALSE, # no end of day pressed
    
    !extra_day_start & (coalesce(begin_hrts_local, end_hrts_local) <= day_end_hrts_local) ~ FALSE,
    !extra_day_start & (coalesce(begin_hrts_local, end_hrts_local) > day_end_hrts_local) ~ TRUE,
    
    extra_day_start & is.na(day_end_hrts_local_2) ~ FALSE,
    
    extra_day_start & (coalesce(begin_hrts_local, end_hrts_local) <= day_end_hrts_local_2) ~ FALSE,
    extra_day_start & (coalesce(begin_hrts_local, end_hrts_local) > day_end_hrts_local_2) ~ TRUE
  )) 

# CC2
block_level_ema_cc2 <- block_level_ema_cc2 %>% 
  arrange(ID_enrolled, study_date, block, begin_hrts_local) %>% 
  group_by(ID_enrolled, study_date, block, ema_type) %>% 
  mutate(extra_ema = row_number() > 1,
         extra_ema_on_block = n() > 1) %>% 
  ungroup() %>% 
  group_by(ID_enrolled, study_date) %>% 
  mutate(extra_ema_on_study_day = any(extra_ema)) %>% 
  ungroup()

block_level_ema_cc2 <- block_level_ema_cc2 %>% 
  mutate(invalid_end_day = case_when(
    is.na(end_hrts_local) ~ FALSE, # undelivered EMA
    is.na(day_end_hrts_local) ~ FALSE, # no end of day pressed
    
    !extra_day_start & (coalesce(begin_hrts_local, end_hrts_local) <= day_end_hrts_local) ~ FALSE,
    !extra_day_start & (coalesce(begin_hrts_local, end_hrts_local) > day_end_hrts_local) ~ TRUE,
    
    extra_day_start & is.na(day_end_hrts_local_2) ~ FALSE,
    
    extra_day_start & (coalesce(begin_hrts_local, end_hrts_local) <= day_end_hrts_local_2) ~ FALSE,
    extra_day_start & (coalesce(begin_hrts_local, end_hrts_local) > day_end_hrts_local_2) ~ TRUE
  )) 

block_level_ema_cc1 <- block_level_ema_cc1 %>% select(-i.timezone_local, -delivered_before_ema_period, -delivered_after_ema_period, -delivered_outside_ema_period) %>% relocate(phone_int_ema, .after = block)
block_level_ema_cc2 <- block_level_ema_cc2 %>% select(-i.timezone_local, -delivered_before_ema_period, -delivered_after_ema_period, -delivered_outside_ema_period) %>% relocate(phone_int_ema, .after = block)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   Step 6: Update phone_int_ema for Days without a Day Start from NA to the corresponding phone_int ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
block_level_ema_cc1 <- block_level_ema_cc1 %>% select(-start_phone_2_hrts_local) %>% 
  left_join(y = dat_master %>% select(ID_enrolled, start_phone_2_hrts_local), by = "ID_enrolled") %>% 
  mutate(start_phone_2_date = as_date(start_phone_2_hrts_local),
         phone_int_ema = case_when(
          !is.na(phone_int_ema) ~ phone_int_ema,
          is.na(start_phone_2_date) ~ "1",
          study_date < start_phone_2_date ~ "1",
          study_date > start_phone_2_date ~ "2",
          block_start_hrts_local < start_phone_2_date ~ "1",
          block_start_hrts_local >= start_phone_2_date ~ "2"
        )) %>% 
  select(-start_phone_2_date, -start_phone_2_hrts_local)

block_level_ema_cc2 <- block_level_ema_cc2 %>% select(-start_phone_2_hrts_local) %>% 
  left_join(y = dat_master %>% select(ID_enrolled, start_phone_2_hrts_local), by = "ID_enrolled") %>% 
  mutate(start_phone_2_date = as_date(start_phone_2_hrts_local),
         phone_int_ema = case_when(
          !is.na(phone_int_ema) ~ phone_int_ema,
          is.na(start_phone_2_date) ~ "1",
          study_date < start_phone_2_date ~ "1",
          study_date > start_phone_2_date ~ "2",
          block_start_hrts_local < start_phone_2_date ~ "1",
          block_start_hrts_local >= start_phone_2_date ~ "2"
        )) %>% 
  select(-start_phone_2_date, -start_phone_2_hrts_local)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 7. Output block-level EMA dataset ready for undelivered EMAs to be investigated ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if(all(test1, test2, test3, test4, test5, test6, test7)){
  print("All tests passed. Saving block-level EMA datasets in the EMA staged folder.")
  save(block_level_ema_cc1, block_level_ema_cc2,
          file = file.path(path_ontrack_ema_staged, "block_level_ema_dataset_pre-undelivered_rsn.RData"))
  saveRDS(dat_master,
          file = file.path(path_ontrack_ema_staged, "masterlist_updated.RDS"))
} else{
  print("Not all tests passed. Needs Review!")
}

# BOTTOM OF PAGE ----