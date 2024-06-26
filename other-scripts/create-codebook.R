# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary:    Update variable names, read in codebook shell, run tests, create codebook
#     
# Inputs:     file.path(path_ontrack_ema_staged, "masterlist_updated.RDS")
#             file.path(path_ontrack_ema_staged, "combined_ema_data.RData")
#             file.path(path_ontrack_ema_staged, "all_ema_data_D1_all_delivered.RData")
#             file.path(path_ontrack_ema_staged, "all_ema_data_D2_per_study_design.RData")
#             file.path(path_ontrack_ema_staged, "combined_online_puffmarker_episode_data.RData")
#             file.path(path_ontrack_ema_staged, "all_ema_data_D3_random_only.RData")
#             file.path(path_ontrack_ema_inputs, "OT_Updated_Variable_Names.xlsx")
#             file.path(path_ontrack_ema_inputs, "codebook_4_input.xlsx")
#     
# Outputs:    file.path(path_ontrack_ema_staged, "codebook.RData")
#             file.path(path_ontrack_ema_staged, "masterlist_final.RData")
#             file.path(path_ontrack_ema_staged, "combined_ema_data_final.RData")
#             file.path(path_ontrack_ema_staged, "all_ema_data_D1_all_delivered_final.RData")
#             file.path(path_ontrack_ema_staged, "all_ema_data_D2_per_study_design_final.RData")
#             file.path(path_ontrack_ema_staged, "combined_online_puffmarker_episode_data_final.RData")
#             file.path(path_ontrack_ema_staged, "all_ema_data_D3_random_only_final.RData")         
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(dplyr)
library(readxl)
library(testthat)
library(tibble)

source("paths.R")
dat_master <- readRDS(file.path(path_ontrack_ema_staged, "masterlist_updated.RDS"))
load(file = file.path(path_ontrack_ema_staged, "combined_ema_data.RData"))
load(file = file.path(path_ontrack_ema_staged, "all_ema_data_D1_all_delivered.RData"))
load(file = file.path(path_ontrack_ema_staged, "all_ema_data_D2_per_study_design.RData"))
load(file = file.path(path_ontrack_ema_staged, "combined_online_puffmarker_episode_data.RData"))
load(file = file.path(path_ontrack_ema_staged, "all_ema_data_D3_random_only.RData"))

# Update datasets to use participant_id as the primary key
cw <- dat_master %>% select(participant_id, ID_enrolled)

all_ema_data_D1_all_delivered <- all_ema_data_D1_all_delivered %>% 
  arrange(ID_enrolled, study_day_int, block) %>% 
  left_join(y = cw, by = "ID_enrolled") %>% 
  relocate(participant_id, .before = ID_enrolled) %>% 
  select(-ID_enrolled)

all_ema_data_D2_per_study_design <- all_ema_data_D2_per_study_design %>% 
  arrange(ID_enrolled, study_day_int, block) %>% 
  left_join(y = cw, by = "ID_enrolled") %>% 
  relocate(participant_id, .before = ID_enrolled) %>% 
  select(-ID_enrolled)

all_ema_data_D3_random_only <- all_ema_data_D3_random_only %>% 
  arrange(ID_enrolled, study_day_int, block) %>% 
  left_join(y = cw, by = "ID_enrolled") %>% 
  relocate(participant_id, .before = ID_enrolled) %>% 
  select(-ID_enrolled)

online_puffmarker_episode_data <- online_puffmarker_episode_data %>% 
  arrange(ID_enrolled, onlinepuffm_unixts) %>% 
  left_join(y = cw, by = "ID_enrolled") %>% 
  relocate(participant_id, .before = ID_enrolled) %>% 
  select(-ID_enrolled)

# Updating variable names per CFH Variable Naming Conventions 
updated_var_names <- read_xlsx(path = file.path(path_ontrack_ema_inputs, "OT_Updated_Variable_Names.xlsx")) %>% 
  select(Original_Variable_Names, Updated_Variable_Names) 

D1_updated_names <- updated_var_names %>% 
  filter(Original_Variable_Names %in% colnames(all_ema_data_D1_all_delivered)) %>% 
  select(Updated_Variable_Names, Original_Variable_Names) %>% 
  deframe

D2_updated_names <- updated_var_names %>% 
  filter(Original_Variable_Names %in% colnames(all_ema_data_D2_per_study_design)) %>% 
  select(Updated_Variable_Names, Original_Variable_Names) %>% 
  deframe

D3_updated_names <- updated_var_names %>% 
  filter(Original_Variable_Names %in% colnames(all_ema_data_D3_random_only)) %>% 
  select(Updated_Variable_Names, Original_Variable_Names) %>% 
  deframe

master_updated_names <- updated_var_names %>% 
  filter(Original_Variable_Names %in% colnames(dat_master)) %>% 
  select(Updated_Variable_Names, Original_Variable_Names) %>% 
  deframe

puffmarker_updated_names <- updated_var_names %>% 
  filter(Original_Variable_Names %in% colnames(online_puffmarker_episode_data)) %>% 
  select(Updated_Variable_Names, Original_Variable_Names) %>% 
  deframe

all_ema_data_D1_all_delivered <- all_ema_data_D1_all_delivered %>% rename( !!! D1_updated_names)
all_ema_data_D2_per_study_design <- all_ema_data_D2_per_study_design %>% rename( !!! D2_updated_names)
all_ema_data_D3_random_only <- all_ema_data_D3_random_only %>% rename( !!! D3_updated_names)
dat_master <- dat_master %>% rename( !!! master_updated_names) %>% select(-in_ematimes)
online_puffmarker_episode_data <- online_puffmarker_episode_data %>% rename ( !!! puffmarker_updated_names)

ema_items_labelled <- ema_items_labelled %>% left_join(y = updated_var_names, by = c("varname_breakfree" = "Original_Variable_Names")) %>% 
  relocate(Updated_Variable_Names, .after = varname_breakfree) %>% 
  select(-varname_breakfree) %>% 
  rename(varname_breakfree = Updated_Variable_Names)

aggregation_rules <- read.csv(file.path(path_ontrack_ema_inputs, "EMA_aggregations_4 read in.csv")) %>% 
  select(Updated_Variables, Aggregation.Method.for.Codebook) %>% rename(vars = Updated_Variables, agg_rule = Aggregation.Method.for.Codebook)

ema_data_all_vars <- tibble(vars = unique(c(names(all_ema_data_D2_per_study_design %>% mutate(ec_extra_ema = NA, .before = ec_extra_ema_on_day) %>% 
                                                    mutate(ec_invalid_end_day = NA, .before = ec_invalid_end_day_on_day)), 
                                            names(all_ema_data_D1_all_delivered), names(all_ema_data_D3_random_only))), 
                            datasource = NA) %>% 
  left_join(y = tibble(vars = names(all_ema_data_D1_all_delivered), all_delivered = TRUE), by = c("vars")) %>% 
  left_join(y = tibble(vars = names(all_ema_data_D2_per_study_design), per_study_design = TRUE), by = c("vars")) %>% 
  left_join(y = tibble(vars = names(all_ema_data_D3_random_only), random_only = TRUE), by = c("vars")) %>% 
  mutate(datasource = case_when(
    all_delivered & per_study_design & random_only ~ "EMA - 1. All Delivered, 2. Per Study Design, and 3. Random Only",
    is.na(all_delivered) & per_study_design & random_only ~ "EMA - 2. Per Study Design, and 3. Random Only",
    is.na(all_delivered) & is.na(per_study_design) & random_only ~ "EMA - 3. Random Only",
    all_delivered & is.na(per_study_design) & is.na(random_only) ~ "EMA - 1. All Delivered"
  )) %>% 
  select(-all_delivered, -per_study_design, -random_only) %>% 
  mutate(id = row_number())

#ema_vars <- tibble(vars = names(all_ema_data_cleaned), datasource = "EMA - All and Random-Only") %>% mutate(id = row_number())
#ema_vars <- tibble(vars = names(ema_data_curated_drops), datasource = "EMA - All and Random-Only") %>% mutate(id = row_number())
master_vars <- tibble(vars = names(dat_master %>% relocate(participant_id, v_cc_indicator, timezone_local, .before = everything())), datasource = "Master") %>% mutate(id = row_number())
puff_vars <- tibble(vars = names(online_puffmarker_episode_data), datasource = "Puffmarker")%>% mutate(id = row_number())
#ema_random_only_vars <- tibble(vars = names(all_ema_data_D3_random_only %>% select(contains("aggreg"))),#, patch_all_records, anhedonia_agg_min, anhedonia_agg_max)),
#                               datasource = "EMA - Random-Only") %>% mutate(id = row_number())

all_vars1 <- bind_rows(master_vars, puff_vars, ema_data_all_vars) %>% 
  mutate(datasource = if_else(vars %in% c("participant_id","v_cc_indicator", "timezone_local"), "All", datasource))

my_dups <- all_vars1 %>% group_by(vars,datasource) %>% filter(n()>1 & datasource != "All")

if (nrow(my_dups)>0){
  print("printout of duplicate variable names (not participant ID or cc_indicator):")
  all_vars1 %>% group_by(vars,datasource) %>% filter(n()>1 & datasource != "All")
} else{print("No Duplicates detected")}

all_vars2 <- all_vars1 %>% 
  group_by(vars,datasource) %>% filter(row_number()==1) %>% ungroup %>% 
  mutate(datasource = factor(datasource, levels = c("All","Master","EMA - 1. All Delivered, 2. Per Study Design, and 3. Random Only", "EMA - 1. All Delivered", 
                                                    "EMA - 2. Per Study Design, and 3. Random Only", "EMA - 3. Random Only","Puffmarker"))) %>% 
  #arrange(datasource,id) %>% 
  mutate(varnum = row_number(),
         varcat = NA_character_,
         varlab = NA_character_,
         vartype = NA_character_,
         vallab = NA_character_,
         # qtype = NA_character_,
         # qtext = NA_character_,
         # resp_opts = NA_character_
  )

# Adding temporary column so the join can merge to the *_aggreg variables too
all_vars2b <- all_vars2 %>% mutate(vars_concat = str_remove(vars, "_aggreg"))


#merge on question label
all_vars3 <- all_vars2b %>% 
  left_join(ema_items_labelled[,c("varname_breakfree","question_text","question_type","question_options","cc1","cc2","question_id_ontrack","scale_name","condition")], 
            by = c("vars_concat"="varname_breakfree")) %>% 
  mutate(varcat = case_when(!is.na(question_id_ontrack) ~ "EMA Item",
                            vars %in% c("ema_type","status","with_any_response","begin_unixts","end_unixts","begin_hrts_UTC","begin_hrts_AmericaChicago","end_hrts_UTC","end_hrts_AmericaChicago") 
                            ~ "EMA Paradata",
                            datasource %in% c("All","Master") ~ "Participant Study Info",
                            vars %in% c('end_hrts_last','minutes_since_last','hours_since_last','cig_flip','othertob_cgr_flip','othertob_ecig_flip','othertob_marij_flip','cig_err','othertob_cgr_err','othertob_ecig_err','othertob_marij_err')
                            ~ "EMA Calculated Variable",
                            datasource == "Puffmarker" ~ "Puffmarker data",
                            T ~ NA_character_),
         varlab = if_else(varcat == "EMA Item","EMA Item -- See Question Text",varlab)) %>% 
  select(-id, #ID is for calculation purposes
         -vartype #skipping vartype for now
  ) %>% select(-vars_concat)

# write_csv(all_vars3, file.path(path_breakfree_staged_data, "codebook_4edits.csv"))

all_vars_manual <- read_xlsx(file.path(path_ontrack_ema_inputs, "codebook_4_input.xlsx"))

all_vars_manual_4update <- all_vars_manual %>% 
  select(vars, varcat, varlab, #vartype, 
         varlab, vallab) %>% 
  semi_join(all_vars3, by = "vars")

all_vars4 <- all_vars3 %>% rows_update(all_vars_manual_4update, by = "vars")

all_vars5 <- all_vars4 %>% 
  left_join(y = aggregation_rules,
            by = c("vars")) %>% 
  relocate(agg_rule, .after = varlab)

test1 <- test_that(desc = "Variable lengths must be less than or equal to 31 characters", code = {
  for (i in 1:nrow(all_vars5)){
    expect_lte(
      object = str_length(all_vars5$vars[i]), 
      expected = 31L
    )
  }
})

test2 <- test_that(desc = "Check that there are no missing labels", code = {
  #nrow(all_vars5 %>% filter(is.na(varlab)))==0
  expect_equal(object = nrow(all_vars5 %>% filter(is.na(varlab))),
               expected = 0L)
})

# Create codebook if both tests pass
if(test1 & test2){
  codebook <- all_vars5 %>% 
    rename(Variables = vars,
           'Data Source' = datasource,
           'Variable Number' = varnum,
           'Variable Category' = varcat,
           'Aggregation Rule for EMAs' = agg_rule,
           'Value Label' = vallab,
           'Question Text' = question_text,
           'Question Type' = question_type,
           'Question Options' = question_options,
           'CC1' = cc1,
           'CC2' = cc2,
           'Scale Name' = scale_name,
           'Condition Required' = condition
    )
  print("Codebook created successfully.")
} else { 
  if(!test1){print(paste("ERROR: variable length greater than 31 characters:", all_vars5$vars[str_length(all_vars5$vars) > 31]))} 
  if (!test2){print(paste("ERROR: missing variable label:", all_vars5 %>% filter(is.na(varlab)) %>% .$vars))}
}

save(codebook,
     file = file.path(path_ontrack_ema_staged, "codebook.RData"))

save(dat_master ,file = file.path(path_ontrack_ema_staged, "masterlist_final.RData"))
save(ema_items_labelled ,file = file.path(path_ontrack_ema_staged, "combined_ema_data_final.RData"))
save(all_ema_data_D1_all_delivered ,file = file.path(path_ontrack_ema_staged, "all_ema_data_D1_all_delivered_final.RData"))
save(all_ema_data_D2_per_study_design ,file = file.path(path_ontrack_ema_staged, "all_ema_data_D2_per_study_design_final.RData"))
save(online_puffmarker_episode_data ,file = file.path(path_ontrack_ema_staged, "combined_online_puffmarker_episode_data_final.RData"))
save(all_ema_data_D3_random_only ,file = file.path(path_ontrack_ema_staged, "all_ema_data_D3_random_only_final.RData"))
