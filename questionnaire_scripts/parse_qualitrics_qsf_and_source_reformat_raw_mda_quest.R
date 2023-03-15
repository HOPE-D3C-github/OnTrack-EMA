library(jsonlite)
library(tidyverse)
library(htm2txt)
library(stringi)
library(haven)
library(readxl)

source('paths.R')

myj <- read_json(file.path(path_ontrack_mda_raw, "OnTrack_MDA_survey_export.qsf"))
if(F){myj$SurveyElements[[26]]}
qualtrics_ids <- myj$SurveyElements %>% map(.,~pluck(.,c("PrimaryAttribute"))) %>% unlist

quest_ids <- myj$SurveyElements %>% map(.,~pluck(.$Payload,c("DataExportTag"), .default = NA)) %>% unlist

quests <- myj$SurveyElements %>% map(.,~pluck(.,c("SecondaryAttribute"), .default = NA)) %>% unlist

quests_long <- myj$SurveyElements %>% map(.,~pluck(.$Payload, c('QuestionText'), .default = NA) %>% trimws %>% htm2txt) %>% unlist

questions_df <- tibble(quest_id = quest_ids,
                       qualtrics_id= qualtrics_ids,
                       quest = quests,
                       quest_long = quests_long
)

choices <- myj$SurveyElements %>% map(.,~.$Payload$Choices)
if(F){
  choices[[624]] %>% names 
  choices[[624]] %>% unlist() %>% unname()}

question_value_df_nested <- questions_df %>% 
  mutate(choices_list_raw = choices,
         choices_val_labels = map(choices_list_raw,~map(.,~.$Display) %>% unlist %>% unname %>% trimws %>% htm2txt),
         choices_val =  map(choices_list_raw,~names(.))) 
question_value_df <- question_value_df_nested %>% select(-choices_list_raw) %>%  unnest(c(choices_val_labels,choices_val),keep_empty = F)
if(F){View(question_value_df)}


# ----------------------------------

if(F){ # for testing the loops below, can initialize using these values
  i_sub <- 2
  i_main <- 180}

# create shell dataframe to add the skip logic info into
skip_logic_df <- data.frame(qualtrics_id = character(), LeftOperand = character(), Operator= character(), reference_qualtrics_id= character(), Conjunction = character(), Description= character())

for (i_main in 1:length(myj$SurveyElements)){
  
  # Iterating across all SurveyElements,
  # add to a growing dataframe all skip logic, 
  # as shown in 'myj[["SurveyElements"]][[__current_main_iteration__]][["Payload"]][["DisplayLogic"]][["0"]]'
  
  surveyelement_i_main <- myj[["SurveyElements"]][[i_main]]
  
  parentQID_i_main = surveyelement_i_main$PrimaryAttribute
  
  logic_i_main <- surveyelement_i_main[["Payload"]][["DisplayLogic"]][['0']]
  
  # Some questions have skip logic referencing multiple questions,
  # so we need to iterate across all available skip logic references
  for (i_sub in 1:length(logic_i_main)){
    if(!is.null(logic_i_main[[as.character(i_sub-1)]])){
      skip_logic_df <- skip_logic_df %>% add_row(
        qualtrics_id = parentQID_i_main,
        LeftOperand = logic_i_main[[as.character(i_sub-1)]][["LeftOperand"]],
        Operator = logic_i_main[[as.character(i_sub-1)]][["Operator"]],
        reference_qualtrics_id = logic_i_main[[as.character(i_sub-1)]][["QuestionID"]],
        Conjunction = logic_i_main[[as.character(i_sub-1)]][["Conjuction"]],
        Description = logic_i_main[[as.character(i_sub-1)]][["Description"]]
      )}
  }
}

# extract the value from the logical expression (that is either SELECTED or NOT SELECTED)
# and the value label from the description
skip_logic_df <- skip_logic_df %>% 
  mutate(reference_value = stri_sub(LeftOperand, -1, -1),
         reference_value_label = str_split(str_split(Description, pattern = "<span class=\"LeftOpDesc\">", simplify = T)[,2], pattern = "</span>", simplify = T)[,1],
         reference_question_text = str_split(str_split(Description, pattern = "<span class=\"QuestionDesc\">", simplify = T)[,2], pattern = "</span>", simplify = T)[,1]
  )

# join the value labels for the referenced value along with the question text for the referenced question
skip_logic_df <- skip_logic_df %>% 
  left_join(
    y = question_value_df %>% select(qualtrics_id, choices_val, reference_quest_id = quest_id, choices_val_labels, reference_quest_long = quest_long),
    by = c("reference_qualtrics_id" = "qualtrics_id", "reference_value" = "choices_val")
  )


skip_logic_df <- questions_df %>% select(quest_id, qualtrics_id, quest_long) %>% 
  right_join(y = skip_logic_df,
             by = c("qualtrics_id"))

skip_logic_df <- skip_logic_df %>% 
  group_by(qualtrics_id) %>% 
  mutate(n_conditionals = n()) %>% 
  ungroup()

# Manually verified that the value labels and the referenced question text extracted from the description match those joined in from question_value_df
skip_logic_df <- skip_logic_df %>%
  select(-c(LeftOperand, Description)) %>%  # remove columns which info has already been extracted and verified
  select(-c(choices_val_labels, reference_quest_long))   # remove duplicate columns used for verification

# reposition columns to make it clearer reading left to right per row  
skip_logic_df <- skip_logic_df %>%  
  relocate(n_conditionals, Conjunction, reference_quest_id, .before = reference_qualtrics_id) %>% 
  relocate(reference_question_text, Operator, .after = reference_qualtrics_id)

# ---------------------------------
# Updated 2/9/2023
# Adding QID_clean variable
# --------------------------------- 

if(F){questions_df %>% arrange(qualtrics_id) %>% View()}

mda_dat_header <- read_csv(file = file.path(path_ontrack_mda_raw, "OnTrack_MDA_Resonses_NumericValues.csv"), n_max = 3, col_names = FALSE, show_col_types = FALSE)

mda_dat_crosswalk <- as.data.frame(t(mda_dat_header), row.names = FALSE)

mda_dat_crosswalk <- set_names(mda_dat_crosswalk, c('mda_var', 'q_text', 'qid_raw_text'))

mda_dat_crosswalk <- mda_dat_crosswalk %>% 
  mutate(qid_raw = str_split(qid_raw_text, pattern = '\"', simplify = TRUE)[,4])

mda_dat_crosswalk <- mda_dat_crosswalk %>% 
  mutate(qid_clean = str_replace_all(qid_raw, pattern = "#", replacement = "_"))

mda_dat_crosswalk <- mda_dat_crosswalk %>% 
  #mutate(qid_root = which( str_detect(str_split(qid_clean, pattern = "_"), pattern = "QID")))
  mutate(
    qid_root_1 = str_split(qid_clean, pattern = "_", simplify = TRUE)[,1],
    qid_root_2 = str_split(qid_clean, pattern = "_", simplify = TRUE)[,2]
  ) %>% 
  mutate(
    qid_root = case_when(
      str_detect(qid_root_1, pattern = "QID") ~ qid_root_1,
      str_detect(qid_root_2, pattern = "QID") ~ qid_root_2,
      T ~ NA_character_
    )) #%>% select(-qid_root_1, -qid_root_2)


mda_dat_crosswalk <- mda_dat_crosswalk %>% 
  mutate(contains_qid = str_detect(qid_clean, pattern = 'QID')) %>% 
  filter(contains_qid & !qid_root %in% c('QID664', 'QID661', "QID665", 'QID663')) %>%   # remove rows that are metadata / not questions from the survey
  select(-contains_qid)

mda_dat_crosswalk <- mda_dat_crosswalk %>% 
  mutate(repeated_root_qid = n() > 1, .by = qid_root)

mda_dat_crosswalk <- mda_dat_crosswalk %>% 
  mutate(
    qid_prefix_int = ifelse(str_detect(qid_clean, '^[:digit:]+_QID'), str_split_fixed(qid_clean, pattern = "_", n =2)[,1], NA_character_),
    qid_sufix_qid = ifelse(str_detect(qid_clean, '^[:digit:]+_QID'), str_split_fixed(qid_clean, pattern = "_", n =2)[,2], NA_character_)
  ) %>% 
  mutate(
    qid_clean = ifelse(!is.na(qid_prefix_int) & !is.na(qid_sufix_qid), paste0(qid_sufix_qid, "_", qid_prefix_int), qid_clean),
    repeated_root_qid = ifelse(!is.na(qid_prefix_int) & !is.na(qid_sufix_qid), FALSE, repeated_root_qid)
  ) %>% 
  mutate(repeated_root_qid = ifelse(qid_clean %in% c("QID436", 'QID436_7_TEXT'), FALSE, repeated_root_qid)) %>%   # First one is a check all that applies, but the csv has all the choices in one column (as a list). Second was grouped in based on same qid root
  select(-qid_prefix_int, -qid_sufix_qid) %>% 
  mutate(is_text_response = str_detect(qid_raw, "TEXT") | qid_root %in% c('QID589', 'QID603') ) # Additional text responses but not picked up by the str_detect

# Split up the repeated and non-repeated qid, because they will be treated differently when merging onto the question_value_df
# They will be combined afterwards
repeated_root_qid <- mda_dat_crosswalk %>% filter(repeated_root_qid)
singular_root_qid <- mda_dat_crosswalk %>% filter(!repeated_root_qid)

# --------------------------------------------------------------------------------------------------------
# Working on repeated root qid first, as they will be most complicated
# --------------------------------------------------------------------------------------------------------
repeated_root_qid <- repeated_root_qid %>% 
  mutate( root_q_text = str_split_fixed(q_text, pattern = '[[:punct:]] - ', n = 2)[,1] , 
          option_choice = str_split_fixed(q_text, pattern = '[[:punct:]] - ', n = 2)[,2] ) %>% 
  mutate(
    root_q_text = ifelse(str_detect(qid_root_2, pattern = "QID"), str_split_fixed(q_text, pattern = ' - ', n = 2)[,2], root_q_text),
    option_choice = ifelse(str_detect(qid_root_2, pattern = "QID"), str_split_fixed(q_text, pattern = ' - ', n = 2)[,1], option_choice)) %>% 
  mutate(option_choice = str_remove(option_choice, pattern = 'CSES04#1 - ')) %>%   # Clean up the CSES04 options
  select(-qid_root_1, -qid_root_2)

repeated_root_qid <- repeated_root_qid %>% 
  mutate(option_choice = str_remove(option_choice, pattern = " - Text"))


#repeated_root_qid_quest_value_df <- question_value_df %>% filter(qualtrics_id %in% repeated_root_qid$qid_root)

repeated_root_qid_quest_value_df <- repeated_root_qid %>% 
  left_join(question_value_df,
            join_by(qid_root == qualtrics_id),
            multiple = "all") # will apply filtering criteria after, but want to visualize the concordance between option choice and choice_val_labels

repeated_root_qid_quest_value_df <- repeated_root_qid_quest_value_df %>% 
  mutate(keep_row = option_choice == choices_val_labels)

# Investigate the merge
if(F){repeated_root_qid_quest_value_df %>% group_by(mda_var, qid_clean) %>% summarise(sum_keep = sum(keep_row)) %>% View()}

if(repeated_root_qid_quest_value_df %>% mutate(sum_keep = sum(keep_row), .by = qid_clean) %>% filter(sum_keep != 1) %>% arrange(qid_clean) %>% nrow() == 0L){
  # When it returns 0 rows, each has 1 match
  repeated_root_qid_quest_value_df <- repeated_root_qid_quest_value_df %>% filter(keep_row)
} else {print('Needs Review: not a 1 to 1 match on option_choice == choices_val_labels')}

# repeated_root_qid_quest_value_df <- repeated_root_qid_quest_value_df %>% 
#   mutate(choices_val_labels = ifelse(is_text_response, 'TEXT_RESPONSE', choices_val_labels),
#          choices_val = ifelse(is_text_response, 'TEXT_RESPONSE', choices_val))


responses_manual_crosswalk <- tribble(
  ~qid_root,         ~choices_val_labels,          ~choices_val,
  'QID296',          'Off',                        'Off',
  'QID296',          'On',                         'On',
  'QID298',          'Off',                        'Off',
  'QID298',          'On',                         'On',
  'QID299',          'Off',                        'Off',
  'QID299',          'On',                         'On',
  'QID300',          'Off',                        'Off',
  'QID300',          'On',                         'On',
  'QID395',          '0',                          'No',
  'QID395',          '1',                          'Yes',
  'QID589',          'TEXT_RESPONSE (we did not validate responses)',              'TEXT_RESPONSE (we did not validate responses)' ,
  'QID603',          'TEXT_RESPONSE  (we did not validate responses)',              'TEXT_RESPONSE  (we did not validate responses)',
  'QID645',          '0',                          'No',
  'QID645',          '1',                          'Yes',
  'QID652',          '0',                          'No, never',
  'QID652',          '1',                          'Yes, but not in the past 3 months',
  'QID652',          '2',                          'Yes, in the past 3 months',
  'QID653',          '0',                          'No, never',
  'QID653',          '1',                          'Yes, but not in the past 3 months',
  'QID653',          '2',                          'Yes, in the past 3 months'
)

repeated_root_qid_quest_value_df <- repeated_root_qid_quest_value_df %>% select(-choices_val_labels, -choices_val) # remove these columns to join in the correct values

repeated_root_qid_quest_value_df <- repeated_root_qid_quest_value_df %>% 
  left_join(y = responses_manual_crosswalk,
            by = "qid_root",
            multiple = 'all')

# Each row in repeated_root_qid_quest_value_df represents a column in the csv dataset. These variables have integer or logical values
# some variables are compound (check all that apply), but map directly to how the data is represented in the csv dataset

# --------------------------------------------------------------------------------------------------------
# Switching over to the singular root qid 
# --------------------------------------------------------------------------------------------------------
singular_root_qid <- singular_root_qid %>% select(-qid_root_1, -qid_root_2)

# qid root will match to qualtrics_id, but the best ID to use is the qid_clean. qid_clean will be crosswalked from qid_root and qualtrics_id
singular_root_qid_quest_value_df <- singular_root_qid %>% 
  left_join( y = question_value_df,
             join_by(qid_root == qualtrics_id),
             multiple = 'all')

# NOTE: the text_responses did not match, but can update the choices_val_labels and choices_val
singular_root_qid_quest_value_df <- singular_root_qid_quest_value_df %>% 
  mutate(choices_val_labels = ifelse(is_text_response, 'TEXT_RESPONSE  (we did not validate responses)', choices_val_labels),
         choices_val = ifelse(is_text_response, 'TEXT_RESPONSE  (we did not validate responses)', choices_val))

# ---------------------------------------------------------------------------------------------------------
# Recombine the repeated_root and singular_root datasets
# ---------------------------------------------------------------------------------------------------------

recombined_cleaned_qid_quest_values <- singular_root_qid_quest_value_df %>% 
  add_row(repeated_root_qid_quest_value_df %>% select(colnames(singular_root_qid_quest_value_df)))


recombined_cleaned_qid_quest_values <- recombined_cleaned_qid_quest_values %>% 
  select(qid_clean, mda_var, quest_id, q_text, quest, quest_long, choices_val_labels, choices_val, everything())


recombined_cleaned_qid_quest_only <- recombined_cleaned_qid_quest_values %>% group_by(qid_clean) %>% filter(row_number() == 1) %>% select(-choices_val_labels, -choices_val)


remove(mda_dat_crosswalk, mda_dat_header, question_value_df, question_value_df_nested, repeated_root_qid, repeated_root_qid_quest_value_df, singular_root_qid, singular_root_qid_quest_value_df)

questions_df <- recombined_cleaned_qid_quest_only
question_value_df <- recombined_cleaned_qid_quest_values

remove(recombined_cleaned_qid_quest_only, recombined_cleaned_qid_quest_values)

# Save questions_df now, because it feeds into the reformatting of the raw MDA questionnaire datasets
# Questions DF: contains question id, qualtrics id, and question text. 1 row per questions
write_rds(questions_df,
          file = file.path(path_ontrack_quest_inputs, 'MDA_questions_parsed.RDS'))
write_dta(questions_df,
          file.path(path_ontrack_quest_inputs, 'MDA_questions_parsed.dta'))
write.csv(questions_df,
          file = file.path(path_ontrack_quest_inputs, 'MDA_questions_parsed.csv'))

# ---------------------------------------------------------------------------------------------------------
# Source in the file that reformats the csv using the qid_clean as the column names
# ---------------------------------------------------------------------------------------------------------

source('reformat_raw_mda_questionnaire_data_files.R')

# ---------------------------------------------------------------------------------------------------------
# QC the range of values and their mappings from the qds against those present in the csv data set
# ---------------------------------------------------------------------------------------------------------


mda_quest_num_val <- readRDS(file = file.path(path_ontrack_quest_inputs, "ontrack_mda_responses_numericvalues_formatted.RDS"))

mda_quest_choice_txt <- readRDS(file = file.path(path_ontrack_quest_inputs, "ontrack_mda_responses_choicetext_formatted.RDS"))

if(F){
  # Use this to initialize variables for testing the for loop
  i <- 40 # column iteration
  j <- 1   # row iteration on column i
}

variable_level_summary_shell <- data.frame(variable_name = character(), in_codebook = logical(), perfect_mapping_to_codebook = logical())
variable_response_level_summary_shell <- data.frame(variable_name = character(), text_value_data = character(), numeric_value_data = character(),
                                                    n_obs = integer()
                                                    #text_value_qsf = character(), numeric_value_qsf = character()          # join these in afterwards. join on text value, full join to bring all possible responses
)

variable_level_summary <- variable_level_summary_shell
variable_response_level_summary <- variable_response_level_summary_shell

for(i in 1:length(colnames(mda_quest_choice_txt))){
  # Iterate over the columns one at a time, collecting the values and their numerical value counterparts
  # Every iteration, i, will add 1 row to variable_level_summary, so i will always map to the corresponding row
  var_i <- colnames(mda_quest_choice_txt)[i]
  
  if(!var_i %in% questions_df$qid_clean){
    # If the variable is not in the qsf codebook, then no validation needed
    # Document it in the variable level summary (not needed in the variable response)
    variable_level_summary <- variable_level_summary %>% add_row(variable_name = var_i, in_codebook = FALSE, perfect_mapping_to_codebook = NA)
  } else {
    # The variable is in the qsf codebook
    # 'perfect_mapping_to_codebook' to be updated later
    variable_level_summary <- variable_level_summary %>% add_row(variable_name = var_i, in_codebook = TRUE, perfect_mapping_to_codebook = NA)
    dat_text_var_i <- mda_quest_choice_txt %>% select(all_of(i))
    dat_numeric_var_i <- mda_quest_num_val %>% select(all_of(i))
    question_value_df_var_i <- question_value_df %>% filter(qid_clean == var_i)
    
    if(question_value_df_var_i$choices_val_labels[1] == 'TEXT_RESPONSE  (we did not validate responses)'){
      # If the variable has text response, then we won't validate.
      # Will be included in the response level, with just one row
      variable_response_level_summary <- variable_response_level_summary %>% 
        add_row(variable_name = var_i, text_value_data = 'TEXT_RESPONSE  (we did not validate responses)', numeric_value_data = 'TEXT_RESPONSE  (we did not validate responses)')
    } else {
      # The variable is NOT a text response
      # Add a new row to the response level summary, for every unique response identified
      # For loop iterating down the rows of variable i
      
      variable_response_level_summary_var_i <- variable_response_level_summary_shell %>% select(-n_obs)
      
      for (j in 1:nrow(dat_text_var_i)){
        dat_text_var_i_row_j <- dat_text_var_i %>% slice(j) %>% as.character()
        dat_numeric_var_i_row_j <- dat_numeric_var_i %>% slice(j) %>% as.character()
        # Add the numeric/text pair for each observation (row)
        variable_response_level_summary_var_i <- variable_response_level_summary_var_i %>% 
          add_row(variable_name = var_i, text_value_data = dat_text_var_i_row_j, numeric_value_data = dat_numeric_var_i_row_j)
      } 
      # End of the j loop: for (j in 1:nrow(dat_text_var_i)){
      # Add variable_response_level_summary_var_i to variable_response_level_summary
      variable_response_level_summary_var_i <- variable_response_level_summary_var_i %>% 
        group_by(variable_name, text_value_data, numeric_value_data) %>% 
        summarize(n_obs = n()) %>% 
        ungroup()
      
      variable_response_level_summary <- variable_response_level_summary %>% add_row(variable_response_level_summary_var_i)
    }
  }
}
variable_response_level_summary_post_loops <- variable_response_level_summary
# Check for mapping of same text to a different numeric, or vice versa (all from within the raw dataset)
variable_response_level_summary <- variable_response_level_summary %>% 
  mutate(n_same_text_different_num = n(), .by = c(variable_name, text_value_data)) %>% 
  mutate(n_same_num_different_text = n(), .by = c(variable_name, numeric_value_data)) 

all(variable_response_level_summary$n_same_text_different_num == 1)  # TRUE
all(variable_response_level_summary$n_same_num_different_text == 1)  # TRUE

variable_response_level_summary <- variable_response_level_summary %>% select(-n_same_text_different_num, -n_same_num_different_text)

# Join the qsf text and numeric values and compare against the numeric values used in the data
variable_response_level_summary <- variable_response_level_summary %>% 
  full_join(
    y = question_value_df %>% select(variable_name = qid_clean, text_value_qsf = choices_val_labels, num_value_qsf = choices_val),
    by = join_by(variable_name, text_value_data == text_value_qsf)
  )

variable_response_level_summary <- variable_response_level_summary %>% arrange(variable_name, numeric_value_data)
variable_response_level_summary <- variable_response_level_summary %>% mutate(response_matches_qsf = numeric_value_data == num_value_qsf)
variable_response_level_summary %>% count(response_matches_qsf)

numeric_values_to_update_in_value_df <- variable_response_level_summary %>% filter(!response_matches_qsf)

nrow(question_value_df)
question_value_df_prejoin <- question_value_df

question_value_df <- question_value_df %>% 
  left_join(y = numeric_values_to_update_in_value_df %>% select(qid_clean = variable_name, text_value_data, numeric_value_data),
            by = join_by(qid_clean, choices_val_labels == text_value_data))

question_value_df <- question_value_df %>% mutate(choices_val = coalesce(numeric_value_data, choices_val))

# Test the updated question_value_df and there should be 100% concordance with variable response level summary
variable_response_level_summary_prefixed <- variable_response_level_summary

variable_response_level_summary <- variable_response_level_summary %>% select(-num_value_qsf, -response_matches_qsf)

# re-run from above 
variable_response_level_summary <- variable_response_level_summary %>% 
  full_join(
    y = question_value_df %>% select(variable_name = qid_clean, text_value_qsf = choices_val_labels, num_value_qsf = choices_val),
    by = join_by(variable_name, text_value_data == text_value_qsf)
  )

variable_response_level_summary <- variable_response_level_summary %>% arrange(variable_name, numeric_value_data)
variable_response_level_summary <- variable_response_level_summary %>% mutate(response_matches_qsf = numeric_value_data == num_value_qsf)
variable_response_level_summary %>% count(response_matches_qsf)
all(variable_response_level_summary$response_matches_qsf, na.rm = TRUE)  # NAs being removed because some text responses and qsf doesn't account for NA due to skip logic etc that we see in the data

question_value_df <- question_value_df %>% select(-numeric_value_data)
question_value_df_pre_2nd_join <- question_value_df
# Investigating further, there are some questions in question_value_df that have choices_val_labels as "".
# Will fix by using the response options (text and numeric) from the raw data
qid_clean_empty_choices_val_labels = question_value_df %>% filter(choices_val_labels == "") %>% .$qid_clean

variable_response_level_summary_subset_fill_in_empty_choice_val <- variable_response_level_summary %>% 
  filter(variable_name %in% qid_clean_empty_choices_val_labels & !is.na(numeric_value_data) & numeric_value_data != 'NA')



question_value_df <- question_value_df %>% 
  left_join(
    y = variable_response_level_summary_subset_fill_in_empty_choice_val %>% select(qid_clean = variable_name, text_value_data, numeric_value_data),
    by = 'qid_clean'
  )

question_value_df <- question_value_df %>% 
  mutate(choices_val_labels = coalesce(text_value_data, choices_val_labels),
         choices_val = coalesce(numeric_value_data, choices_val)) %>% 
  select(-text_value_data, -numeric_value_data)

# Run the merge one last time to show final concordance
variable_response_level_summary <- variable_response_level_summary %>% select(-num_value_qsf, -response_matches_qsf)

# re-run from above 
variable_response_level_summary <- variable_response_level_summary %>% 
  full_join(
    y = question_value_df %>% select(variable_name = qid_clean, text_value_qsf = choices_val_labels, num_value_qsf = choices_val),
    by = join_by(variable_name, text_value_data == text_value_qsf)
  )

variable_response_level_summary <- variable_response_level_summary %>% arrange(variable_name, numeric_value_data)
variable_response_level_summary <- variable_response_level_summary %>% mutate(response_matches_qsf = numeric_value_data == num_value_qsf)
variable_response_level_summary %>% count(response_matches_qsf)
all(variable_response_level_summary$response_matches_qsf, na.rm = TRUE)  # NAs being removed because some text responses and qsf doesn't account for NA due to skip logic etc that we see in the data

# ----------------------
# Export datasets
# ----------------------

if(T){
  # Questions value DF: contains question id, qualtrics id, question text, response values, and response value labels. 1 row per response per question
  write_rds(question_value_df,
            file = file.path(path_ontrack_quest_inputs, 'MDA_questions_w_responses_parsed.RDS'))
  write_dta(question_value_df,
            file.path(path_ontrack_quest_inputs, 'MDA_questions_w_responses_parsed.dta'))
  write.csv(question_value_df,
            file = file.path(path_ontrack_quest_inputs, 'MDA_questions_w_responses_parsed.csv'))
  
  # Skip logic DF
  write_rds(skip_logic_df,
            file = file.path(path_ontrack_quest_inputs, 'MDA_skip_logic_parsed.RDS'))
  write_dta(skip_logic_df,
            file.path(path_ontrack_quest_inputs, 'MDA_skip_logic_parsed.dta'))
  write.csv(skip_logic_df,
            file = file.path(path_ontrack_quest_inputs, 'MDA_skip_logic_parsed.csv'))
  
  # Variable response level summary (from the data) after merging to the question_value_df (from the qsf) but before updating question_value_df -
  # showing the 299 responses with different numeric encoding as well as the 
  # responses that were not captured in the qsf (only captured "" as the text value label)
  write_rds(variable_response_level_summary_prefixed,
            file = file.path(path_ontrack_quest_output_4_analysis, 'variable_response_level_summary_prefixed.RDS'))
  write_dta(variable_response_level_summary_prefixed,
            file.path(path_ontrack_quest_output_4_analysis, 'variable_response_level_summary_prefixed.dta'))
  write.csv(variable_response_level_summary_prefixed,
            file = file.path(path_ontrack_quest_output_4_analysis, 'variable_response_level_summary_prefixed.csv'))
  
  # Variable response level summary (from the data) after merging to the question_value_df (from the qsf) and after updating question_value_df
  write_rds(variable_response_level_summary,
            file = file.path(path_ontrack_quest_output_4_analysis, 'variable_response_level_summary_postfixed.RDS'))
  write_dta(variable_response_level_summary,
            file.path(path_ontrack_quest_output_4_analysis, 'variable_response_level_summary_postfixed.dta'))
  write.csv(variable_response_level_summary,
            file = file.path(path_ontrack_quest_output_4_analysis, 'variable_response_level_summary_postfixed.csv'))
}



