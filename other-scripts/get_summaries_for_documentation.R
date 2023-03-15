library(dplyr)
library(scales)


source('paths.R')

dat_master <- readRDS(file = file.path(path_ontrack_ema_output_4_analysis, "masterlist.rds"))
ema_data_D1 <- readRDS(file = file.path(path_ontrack_ema_output_dm, "all_ema_data-1-all_delivered.rds"))
ema_data_D2 <- readRDS(file = file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-2-per_study_design.rds"))
ema_data_D3 <- readRDS(file = file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-3-random_only_ema.rds"))


# Get Percentage of Total EMAs Affected by Extra EMA
ema_data_D1 %>% count(ec_extra_ema) %>% mutate(percnt = round(n/sum(n) * 100, digits = 2))

# Get Percentage of Total Study Days Affected by Extra EMA
ema_data_D1 %>% filter(row_number() == 1, .by = c(ID_enrolled, ec_study_day_int)) %>% count(ec_extra_ema_on_day) %>% mutate(percnt = round(n/sum(n) * 100, digits = 2))

# Get Percentage of Total Participants Affected by Extra EMA
ema_data_D1 %>% summarise(pt_any_extra_ema = any(ec_extra_ema), .by = ID_enrolled) %>% count(pt_any_extra_ema) %>% mutate(percnt = round(n/sum(n) * 100, digits = 2))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Get Percentage of Total EMAs Affected by Invalid End Day
ema_data_D1 %>% count(ec_invalid_end_day) %>% mutate(percnt = round(n/sum(n) * 100, digits = 2))

# Get Percentage of Total Study Days Affected by Invalid End Day
ema_data_D1 %>% filter(row_number() == 1, .by = c(ID_enrolled, ec_study_day_int)) %>% count(ec_invalid_end_day_on_day) %>% mutate(percnt = round(n/sum(n) * 100, digits = 2))

# Get Percentage of Total Participants Affected by Invalid End Day
ema_data_D1 %>% summarise(pt_any_extra_ema = any(ec_invalid_end_day), .by = ID_enrolled) %>% count(pt_any_extra_ema) %>% mutate(percnt = round(n/sum(n) * 100, digits = 2))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Dataset D1 summary info

# Number of Observations
ema_data_D1 %>% nrow()

# Number of Delivered / Undelivered EMAs
ema_data_D1 %>% count(ec_ema_delivered)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Dataset D2 summary info

# Number of Observations
ema_data_D2 %>% nrow()

# Number of Delivered / Undelivered EMAs
ema_data_D2 %>% count(ec_ema_delivered)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Dataset D3 summary info

# Number of Observations
ema_data_D3 %>% nrow()

# Number of Delivered / Undelivered EMAs
ema_data_D3 %>% count(ec_ema_delivered)

