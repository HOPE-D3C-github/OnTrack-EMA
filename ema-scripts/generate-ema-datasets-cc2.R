# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary:  Compile all EMA types for all Participants
#     
# Inputs:   file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData")
#           file.path(path_ontrack_ema_staged, "ema_questionnaires_cc1.RData")
#     
# Outputs:  file.path(path_ontrack_ema_staged, "all_ema_data_cc1.RData")
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(dplyr)
library(lubridate)
library(purrr)
source("paths.R")
source(file.path("collect-functions","new-variable-utils.R"))

# Load previously parsed data
# load(file.path(path_ontrack_staged, "masterlist.RData"))
load(file.path(path_ontrack_ema_staged, "ema_raw_data_cc2.RData"))
load(file.path(path_ontrack_ema_staged, "ema_questionnaires_cc2.RData"))

# Participant IDs for data collected using CC2 platform
ema_subfolder_names_all <- list.files(path_ontrack_cc2_input_data)
# remove files (retain only folders) and remove folders for test participants
subset_indices <- replace_na(str_split_fixed(ema_subfolder_names_all, "_", n = 2)[,1] == "ses" & !str_detect(ema_subfolder_names_all, "_test_"))
ema_subfolder_names <- ema_subfolder_names_all[subset_indices]
ids_cc2 <- mixedsort(ema_subfolder_names)
remove(ema_subfolder_names_all, subset_indices, ema_subfolder_names)

# Create an empty list to contain all the parsed EMA data
list_all_ema_data <- list()

# -----------------------------------------------------------------------------
# Select EMA type on which we will continue the parsing process: Random EMA
# -----------------------------------------------------------------------------
all_status_files_cc2 <- all_random_ema_status_files_cc2
all_response_files_cc2 <- all_random_ema_response_files_cc2
ema_items_cc2 <- random_ema_items_cc2
use_ema_type <- "RANDOM"

source(file.path("ema-scripts", "parse-ema-responses-cc2.R"))
list_all_ema_data <- append(list_all_ema_data, list(df_collect_all))

# -----------------------------------------------------------------------------
# Select EMA type on which we will continue the parsing process: Smoking EMA
# -----------------------------------------------------------------------------
all_status_files_cc2 <- all_smoking_ema_status_files_cc2
all_response_files_cc2 <- all_smoking_ema_response_files_cc2
ema_items_cc2 <- smoking_ema_items_cc2
use_ema_type <- "SMOKING"

source(file.path("ema-scripts", "parse-ema-responses-cc2.R"))
list_all_ema_data <- append(list_all_ema_data, list(df_collect_all))

# -----------------------------------------------------------------------------
# Select EMA type on which we will continue the parsing process: Stress EMA
# -----------------------------------------------------------------------------
all_status_files_cc2 <- all_stress_ema_status_files_cc2
all_response_files_cc2 <- all_stress_ema_response_files_cc2
ema_items_cc2 <- stress_ema_items_cc2
use_ema_type <- "STRESS"

source(file.path("ema-scripts", "parse-ema-responses-cc2.R"))
list_all_ema_data <- append(list_all_ema_data, list(df_collect_all))

# -----------------------------------------------------------------------------
# Aggregate data coming from all kinds of EMA
# -----------------------------------------------------------------------------
all_ema_data <- do.call(rbind, list_all_ema_data)
all_ema_data <- all_ema_data %>% arrange(participant_id, end_unixts)

# -----------------------------------------------------------------------------
# Calculate length of Survey Completion Time
# -----------------------------------------------------------------------------

all_ema_data <- all_ema_data %>% 
  mutate (survey_length_minutes = as.numeric(end_hrts_AmericaChicago - begin_hrts_AmericaChicago)/60)

# # -----------------------------------------------------------------------------
# # Adding curated withdrew date to be used in exclusion rules
# # -----------------------------------------------------------------------------
# dat_master <- dat_master %>% 
#   mutate(
#     last_day_date_w_withdrew = case_when(
#       withdrew_date < last_day_date ~ withdrew_date,
#       T ~ last_day_date),
#     .after = withdrew_date)

# -----------------------------------------------------------------------------
# Apply our exclusion rules
# -----------------------------------------------------------------------------
# all_ema_data <- semi_join(x = all_ema_data, 
#                           y = dat_master %>%
#                             select(participant_id, withdrew) %>%
#                             filter(withdrew == 0), 
#                           by = "participant_id")

# # Note that not all EMAs have a value for begin_hrts
# # For example, 'MISSED' EMAs will not have begin_hrts
# # since this timestamp will not be applicable to that EMA
# all_ema_data <- all_ema_data %>% 
#   left_join(x = .,
#             y = dat_master %>% 
#               select(participant_id, cc_indicator, last_day_date,
#                      first_day_date, last_day_date_w_withdrew),
#             by = "participant_id") %>%
#   filter(as_date(end_hrts_AmericaChicago) >= first_day_date) %>%  
#   filter(as_date(end_hrts_AmericaChicago) <= last_day_date_w_withdrew) %>%   # Right censor on withdrew date
#   #filter(as_date(end_hrts_AmericaChicago) <= last_day_date) %>%  # Right censor on last_day_date only
#   select(-first_day_date, -last_day_date_w_withdrew, -last_day_date) %>%
#   select(participant_id, cc_indicator, everything()) %>% 
#   arrange(participant_id, end_unixts)

# -----------------------------------------------------------------------------
# Save output
# -----------------------------------------------------------------------------
all_ema_data_cc2 <- all_ema_data
save(all_ema_data_cc2, file = file.path(path_ontrack_ema_staged, "all_ema_data_cc2.RData"))


