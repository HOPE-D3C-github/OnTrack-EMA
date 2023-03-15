# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Reformatting the MDA raw questionnaire data
# to use the qid_clean variable names
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
library(dplyr)
library(haven)
library(janitor)
library(readr)
library(tibble)

source('paths.R')
mda_questions_parsed <- readRDS(file = file.path(path_ontrack_quest_inputs, "MDA_questions_parsed.RDS"))

# ------------------------------------------------------------------------------------------------------------------------------

header <- read_csv(file = file.path(path_ontrack_mda_raw, "OnTrack_MDA_Responses_ChoiceText.csv"), n_max = 2)

header_t <- t(header) %>% as.data.frame() %>% rownames_to_column(var = 'original_var_name') %>% rename(qid_raw_text = V2) %>% select(-V1)

header_t <- header_t %>% full_join(y = mda_questions_parsed %>% select(qid_clean, qid_raw_text), by = "qid_raw_text")

header_t <- header_t %>% mutate(updated_var_name = coalesce(qid_clean, original_var_name))

header_t <- header_t %>% mutate(updated_var_name = ifelse(updated_var_name == "Duration (in seconds)", "Duration_in_seconds", updated_var_name))


updated_var_names <- header_t$updated_var_name

# ------------------------------------------------------------------------------------------------------------------------------
raw_data_choice_text <- read_csv(file = file.path(path_ontrack_mda_raw, "OnTrack_MDA_Responses_ChoiceText.csv"), skip = 2)
updated_data_choice_text <- raw_data_choice_text
colnames(updated_data_choice_text) <- updated_var_names


raw_data_numeric_values <- read_csv(file = file.path(path_ontrack_mda_raw, "OnTrack_MDA_Resonses_NumericValues.csv"), skip = 2)
updated_data_numeric_value <- raw_data_numeric_values
colnames(updated_data_numeric_value) <- updated_var_names

# --------------------------------------------------------------------------------------------------------------------------------

write_csv(updated_data_choice_text,
          file.path(path_ontrack_quest_inputs, "ontrack_mda_responses_choicetext_formatted.csv"))
saveRDS(updated_data_choice_text,
        file = file.path(path_ontrack_quest_inputs, "ontrack_mda_responses_choicetext_formatted.RDS"))
write_dta(updated_data_choice_text,
          file.path(path_ontrack_quest_inputs, "ontrack_mda_responses_choicetext_formatted.dta"))

write_csv(updated_data_numeric_value,
          file.path(path_ontrack_quest_inputs, "ontrack_mda_responses_numericvalues_formatted.csv"))
saveRDS(updated_data_numeric_value,
        file = file.path(path_ontrack_quest_inputs, "ontrack_mda_responses_numericvalues_formatted.RDS"))
write_dta(updated_data_numeric_value,
          file.path(path_ontrack_quest_inputs, "ontrack_mda_responses_numericvalues_formatted.dta"))

