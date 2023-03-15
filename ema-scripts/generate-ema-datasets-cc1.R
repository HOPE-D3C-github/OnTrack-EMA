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
library(magrittr)
library(purrr)
source("paths.R")
source(file.path("collect-functions","new-variable-utils.R"))

# We've begun a preliminary parsing process; load in data from that step
load(file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData"))
load(file.path(path_ontrack_ema_staged, "ema_questionnaires_cc1.RData"))

# Create an empty list to contain all the parsed EMA data
list_all_ema_data <- list()

# -----------------------------------------------------------------------------
# Select EMA type on which we will continue the parsing process: Random EMA
# -----------------------------------------------------------------------------
all_response_files_cc1 <- all_random_ema_response_files_cc1
use_ema_type <- "RANDOM"
ema_items_cc1 <- random_ema_items_cc1
source(file.path("ema-scripts", "parse-ema-responses-cc1.R"))
list_all_ema_data <- append(list_all_ema_data, list(df_collect_all))

# -----------------------------------------------------------------------------
# Select EMA type on which we will continue the parsing process: Smoking EMA
# -----------------------------------------------------------------------------
all_response_files_cc1 <- all_smoking_ema_response_files_cc1
use_ema_type <- "SMOKING"
ema_items_cc1 <- smoking_ema_items_cc1
source(file.path("ema-scripts", "parse-ema-responses-cc1.R"))
list_all_ema_data <- append(list_all_ema_data, list(df_collect_all))

# -----------------------------------------------------------------------------
# Select EMA type on which we will continue the parsing process: Stress EMA
# -----------------------------------------------------------------------------
all_response_files_cc1 <- all_stress_ema_response_files_cc1
use_ema_type <- "STRESS"
ema_items_cc1 <- stress_ema_items_cc1
source(file.path("ema-scripts", "parse-ema-responses-cc1.R"))
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

# -----------------------------------------------------------------------------
# Save output
# -----------------------------------------------------------------------------
all_ema_data_cc1 <- all_ema_data
save(all_ema_data_cc1, file = file.path(path_ontrack_ema_staged, "all_ema_data_cc1.RData"))


