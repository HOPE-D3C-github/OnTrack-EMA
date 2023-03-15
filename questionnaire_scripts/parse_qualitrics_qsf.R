library(jsonlite)
library(tidyverse)
library(htm2txt)
library(stringi)
library(haven)

source("paths.R")

myj <- read_json(file.path(path_ontrack_mda_raw , "OnTrack_MDA_survey_export.qsf") )
myj$SurveyElements[[26]]

qualtrics_ids <- myj$SurveyElements %>% map(.,~pluck(.,c("PrimaryAttribute"))) %>% unlist

quest_ids <- myj$SurveyElements %>% map(.,~pluck(.$Payload,c("DataExportTag")) %>% if_else(!is.null(.),NA_character_,.)) %>% unlist

#quest_ids <- myj$SurveyElements %>% map(.,~pluck(.$Payload,c("DataExportTag"))) %>% unlist


quests <- myj$SurveyElements %>% map(.,~pluck(.,c("SecondaryAttribute")) %>% if_else(is.null(.),NA_character_,.)) %>% unlist
quests_long <- myj$SurveyElements %>% map(.,~.$Payload$QuestionText %>% if_else(is.null(.),NA_character_,.) %>% trimws %>% htm2txt) %>% unlist



questions_df <- tibble(quest_id = quest_ids,
                       qualtrics_id= qualtrics_ids,
                       quest = quests,
                       quest_long = quests_long,
                       #skip_logic_raw_expr = skip_logic_raw_expr,
                       #skip_logic_operator = skip_logic_operator
)

choices <- myj$SurveyElements %>% map(.,~.$Payload$Choices)
choices[[624]] %>% names 
choices[[624]] %>% unlist() %>% unname()

(question_value_df_nested <- questions_df %>% 
    mutate(choices_list_raw = choices,
           choices_val_labels = map(choices_list_raw,~map(.,~.$Display) %>% unlist %>% unname %>% trimws %>% htm2txt),
           choices_val =  map(choices_list_raw,~names(.)))) 
question_value_df <- question_value_df_nested %>% select(-choices_list_raw) %>%  unnest(c(choices_val_labels,choices_val),keep_empty = F)
question_value_df


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

# ----------------------
# Export datasets
# ----------------------

if(F){
  # Questions DF: contains question id, qualtrics id, and question text. 1 row per questions
  write_rds(questions_df,
            file = file.path('../../OnTrack Pre-Curated Data/Questionnaire Data/inputs', 'MDA_questions_parsed.RDS'))
  write_dta(questions_df,
            file.path('../../OnTrack Pre-Curated Data/Questionnaire Data/inputs', 'MDA_questions_parsed.dta'))
  write.csv(questions_df,
            file = file.path('../../OnTrack Pre-Curated Data/Questionnaire Data/inputs', 'MDA_questions_parsed.csv'))
  
  # Questions value DF: contains question id, qualtrics id, question text, response values, and response value labels. 1 row per response per question
  write_rds(question_value_df,
            file = file.path('../../OnTrack Pre-Curated Data/Questionnaire Data/inputs', 'MDA_questions_w_responses_parsed.RDS'))
  write_dta(question_value_df,
            file.path('../../OnTrack Pre-Curated Data/Questionnaire Data/inputs', 'MDA_questions_w_responses_parsed.dta'))
  write.csv(question_value_df,
            file = file.path('../../OnTrack Pre-Curated Data/Questionnaire Data/inputs', 'MDA_questions_w_responses_parsed.csv'))
  
  # Skip logic DF
  write_rds(skip_logic_df,
            file = file.path('../../OnTrack Pre-Curated Data/Questionnaire Data/inputs', 'MDA_skip_logic_parsed.RDS'))
  write_dta(skip_logic_df,
            file.path('../../OnTrack Pre-Curated Data/Questionnaire Data/inputs', 'MDA_skip_logic_parsed.dta'))
  write.csv(skip_logic_df,
            file = file.path('../../OnTrack Pre-Curated Data/Questionnaire Data/inputs', 'MDA_skip_logic_parsed.csv'))
  
  
}



