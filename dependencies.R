# This file is to be sourced at the beginning of 
# "data_curation_pipeline-using_dependencies.R"
# to load in the dataframe containing the file names
# and their respective inputs and outputs.
# The dataframe must be ordered to match the hierarchy
# in the pipeline, so the earliest files run first
library(dplyr)
library(stringr)
library(tidyr)
library(ggplot2)

source("paths.R")

vars_pre <- ls()

# -------------------------------------------------------
# Format example
# -------------------------------------------------------
# File Name (.R or .rmd)
# Inputs (list all)
# Outputs (list all)

read_ema_cc1 <- list(
  file.name = "ema-scripts/read-raw-data-cc1.R",
  domain = "EMA",
  inputs = NA,
  outputs = file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData")
)

parse_ema_quest_cc1 <- list(
  file.name = "ema-scripts/parse-ema-questionnaire-cc1.R",
  domain = "EMA",
  inputs = file.path(path_ontrack_visit_staged, "ema_responses_raw_data_cc1.RData"),
  outputs = file.path(path_ontrack_ema_staged, "ema_questionnaires_cc1.RData")
)

gen_ema_dat_cc1 <- list(
  file.name = "ema-scripts/generate-ema-datasets-cc1.R",
  domain = "EMA",
  inputs = c(file.path(path_ontrack_ema_staged, "ema_questionnaires_cc1.RData"), 
             file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData"),
             file.path("ema-scripts", "parse-ema-responses-cc1.R"),
             file.path("ema-scripts", "grab-single-response-items-cc1.R"), 
             file.path("ema-scripts", "grab-multiple-response-items-cc1.R")),
  outputs = file.path(path_ontrack_ema_staged, "all_ema_data_cc1.RData")
)

read_ema_cc2 <- list(
  file.name = "ema-scripts/read-raw-data-cc2.R",
  domain = "EMA",
  inputs = NA,
  outputs = c( file.path(path_ontrack_ema_staged, "day_start_and_end_cc2.RData"),
               file.path(path_ontrack_ema_staged, "ema_raw_data_cc2.RData"))
)

parse_ema_quest_cc2 <- list(
  file.name = "ema-scripts/parse-ema-questionnaire-cc2.R",
  domain = "EMA",
  inputs = file.path(path_ontrack_ema_staged, "ema_raw_data_cc2.RData"),
  outputs = file.path(path_ontrack_ema_staged, "ema_questionnaires_cc2.RData")
)

gen_ema_dat_cc2 <- list(
  file.name = "ema-scripts/generate-ema-datasets-cc2.R",
  domain = "EMA",
  inputs = c(file.path(path_ontrack_ema_staged, "ema_questionnaires_cc2.RData"), 
             file.path(path_ontrack_ema_staged, "ema_raw_data_cc2.RData"),
             file.path("ema-scripts", "parse-ema-responses-cc2.R"),
             file.path("ema-scripts", "grab-single-response-items-cc2.R"), 
             file.path("ema-scripts", "grab-multiple-response-items-cc2.R")),
  outputs = file.path(path_ontrack_ema_staged, "all_ema_data_cc2.RData")
)

ema_first_last_dates <- list(
  file.name = "visit_outcomes_scripts/grab_ema_dates.R",  
  # this file uses EMA data for the visit outcomes portion of the pipeline, 
  # so the inputs come from the EMA staged, but the output is in the visit outcomes stages
  domain = "Visit",
  inputs = c(file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData"), 
             file.path(path_ontrack_ema_staged, "ema_raw_data_cc2.RData")),  
  outputs = file.path(path_ontrack_visit_staged, "ema_first_last_dates.rds") 
)

compile_mda_sas_files <- list(
  file.name = "visit_outcomes_scripts/compile_MDA_SAS_files.R",
  domain = "Visit",
  inputs = NA, 
  outputs = file.path(path_ontrack_visit_staged,"list_of_all_sas_data_from_MDA.Rds")
)

mda_visit_data_prep <- list(
  file.name = "visit_outcomes_scripts/OnTrack-visit_data_curation-MDA_v1.Rmd",
  domain = "Visit",
  inputs = c(
    file.path(path_ontrack_visit_staged, "ema_first_last_dates.rds"),
    file.path(path_ontrack_visit_staged,"list_of_all_sas_data_from_MDA.Rds")),
  outputs = c(file.path(path_ontrack_visit_outputs, "data_all_mda.RDS"),
              file.path(path_ontrack_visit_outputs, "enrolled_mda.RDS"))
)

utah_visit_data_prep <- list(
  file.name = "visit_outcomes_scripts/OnTrack-visit_data_curation-Utah_v1.Rmd",
  domain = "Visit",
  inputs = file.path(path_ontrack_visit_staged, "ema_first_last_dates.rds"),
  outputs = c(file.path(path_ontrack_visit_outputs, "utah_crosswalk.rds"),
              file.path(path_ontrack_visit_outputs, "utah_data_all.rds"),
              file.path(path_ontrack_visit_outputs, "utah_data_checklist_clean.rds"),
              file.path(path_ontrack_visit_outputs, "utah_questionnaire.rds"),
              file.path(path_ontrack_visit_outputs, "utah_screener.rds"),
              file.path(path_ontrack_visit_outputs, "utah_event_clean_wide.rds"),
              file.path(path_ontrack_visit_outputs, "utah_visit_comments.rds"))
)

combine_dat_masters <- list(
  file.name = "visit_outcomes_scripts/combine_dat_masters.R",
  domain = "Visit",
  inputs = c(file.path(path_ontrack_visit_outputs, "enrolled_mda.RDS"),
             file.path(path_ontrack_visit_outputs, "utah_data_all.rds")),
  outputs = file.path(path_ontrack_visit_outputs, "masterlist.RData")
)

read_battery_data <- list(
  file.name = "other-scripts/read_in_battery_data.R",
  domain = "EMA",
  inputs = file.path(path_ontrack_visit_outputs, "masterlist.RData"),
  outputs = file.path(path_ontrack_ema_staged, "filtered_battery_data.RData")
)

process_battery_data <- list(
  file.name = "other-scripts/processing_battery_data_into_bins.R",
  domain = "EMA",
  inputs = c(file.path(path_ontrack_visit_outputs, "masterlist.RData"),
             file.path(path_ontrack_ema_staged, "filtered_battery_data.RData")),
  outputs = file.path(path_ontrack_ema_staged, "battery_data_binned.RData")
  )

calculate_study_day_ema_blocks <- list(
  file.name = "other-scripts/calculate_study_day_ema_blocks-updated.R",
  domain = "EMA",
  inputs = c(file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData"),
             file.path(path_ontrack_ema_staged, "day_start_and_end_cc2.RData"),
             file.path(path_ontrack_visit_outputs, "masterlist.RData"),
             file.path(path_ontrack_ema_staged, "all_ema_data_cc1.RData"),
             file.path(path_ontrack_ema_staged, "all_ema_data_cc2.RData")
             ),
  outputs = c(file.path(path_ontrack_ema_staged, "block_level_ema_dataset_pre-undelivered_rsn.RData"),
              file.path(path_ontrack_ema_staged, "masterlist_updated.RDS"))
)

generate_block_level_dataset_cc1 <- list(
 file.name = "other-scripts/generate_block_level_dataset-cc1.R",
 domain = "EMA",
 inputs = c(
   file.path(path_ontrack_ema_staged, "block_level_ema_dataset_pre-undelivered_rsn.RData"),
   file.path(path_ontrack_ema_staged, "masterlist_updated.RDS"),
   file.path(path_ontrack_ema_staged, "battery_data_binned.RData"),
   file.path(path_ontrack_ema_staged, "ema_responses_raw_data_cc1.RData")
 ),
 outputs = file.path(path_ontrack_ema_staged, "block_level_ema_dataset_cc1.RDS")
)

generate_block_level_dataset_cc2 <- list(
  file.name = "other-scripts/generate_block_level_dataset-cc2.R",
  domain = "EMA",
  inputs = c(
    file.path(path_ontrack_ema_staged, "block_level_ema_dataset_pre-undelivered_rsn.RData"),
    file.path(path_ontrack_ema_staged, "masterlist_updated.RDS"),
    file.path(path_ontrack_ema_staged, "battery_data_binned.RData"),
    file.path(path_ontrack_ema_staged, "ema_raw_data_cc2.RData")
  ),
  outputs = file.path(path_ontrack_ema_staged, "block_level_ema_dataset_cc2.RDS")
)

biomarker_read_raw_cc1 <- list(
  file.name = file.path("biomarker-smoking-scripts","read-raw-data-cc1.R"),
  domain = "Biomarker",
  inputs = NA,
  outputs = file.path(path_ontrack_ema_staged, "online_puffmarker_episode_raw_data_cc1.RData")
)

biomarker_gen_dat_cc1 <- list(
  file.name = file.path("biomarker-smoking-scripts","generate-biomarker-smoking-dataset-cc1.R"),
  domain = "Biomarker",
  inputs = c(file.path(path_ontrack_ema_staged, "online_puffmarker_episode_raw_data_cc1.RData"),
             file.path(path_ontrack_ema_staged, "masterlist_updated.RDS")),
  outputs = file.path(path_ontrack_ema_staged, "online_puffmarker_episode_data_cc1.RData")
)

biomarker_read_raw_cc2 <- list(
  file.name = file.path("biomarker-smoking-scripts","read-raw-data-cc2.R"),
  domain = "Biomarker",
  inputs = NA,
  outputs = file.path(path_ontrack_ema_staged, "online_puffmarker_episode_raw_data_cc2.RData")
)

biomarker_gen_dat_cc2 <- list(
  file.name = file.path("biomarker-smoking-scripts","generate-biomarker-smoking-dataset-cc2.R"),
  domain = "Biomarker",
  inputs = c(file.path(path_ontrack_ema_staged, "online_puffmarker_episode_raw_data_cc2.RData"),
             file.path(path_ontrack_ema_staged, "masterlist_updated.RDS")),
  outputs = file.path(path_ontrack_ema_staged, "online_puffmarker_episode_data_cc2.RData")
)

combine_cc1_cc2 <- list(
  file.name = file.path("other-scripts", "combine-cc1-cc2.R"),
  domain = "EMA",
  inputs = c(file.path(path_ontrack_ema_staged,"ema_questionnaires_cc1.RData"),
             file.path(path_ontrack_ema_staged,"ema_questionnaires_cc2.RData"),
             file.path(path_ontrack_ema_inputs,"ema_items_labelled_4_input.xlsx"),
             file.path(path_ontrack_ema_staged, "block_level_ema_dataset_cc1.RDS"),
             file.path(path_ontrack_ema_staged, "block_level_ema_dataset_cc2.RDS"),
             file.path(path_ontrack_ema_staged, "online_puffmarker_episode_data_cc1.RData"),
             file.path(path_ontrack_ema_staged, "online_puffmarker_episode_data_cc2.RData") ),
  outputs = c(file.path(path_ontrack_ema_staged, "combined_ema_data.RData"),
              file.path(path_ontrack_ema_staged, "combined_online_puffmarker_episode_data.RData"))
)

create_recalc_ema_vars <- list(
  file.name = "ema-scripts/create-recalculated-ema-vars.R",
  domain = "EMA",
  inputs = file.path(path_ontrack_ema_staged, "combined_ema_data.RData"),
  outputs = file.path(path_ontrack_ema_staged, "all_ema_data_D1_all_delivered.RData")
)

create_ema_per_study_design <- list(
  file.name = "ema-scripts/create-ema-per-study-design.R",
  domain = "EMA",
  inputs = c(file.path(path_ontrack_ema_inputs, "EMA_aggregations_4 read in.csv"),
             file.path(path_ontrack_ema_staged, "all_ema_data_D1_all_delivered.RData")),
  outputs = file.path(path_ontrack_ema_staged, "all_ema_data_D2_per_study_design.RData")
)

create_random_only_ema <- list(
  file.name = "ema-scripts/create-random-only-ema.R", 
  domain = "EMA",
  inputs = c(file.path(path_ontrack_ema_inputs, "EMA_aggregations_4 read in.csv"),
             file.path(path_ontrack_ema_staged, "all_ema_data_D2_per_study_design.RData")),
  outputs = file.path(path_ontrack_ema_staged, "all_ema_data_D3_random_only.RData")
)

create_codebook <- list(
  file.name = "other-scripts/create-codebook.R",
  domain = "EMA",
  inputs = c(
    file.path(path_ontrack_ema_staged, "masterlist_updated.RDS"),
    file.path(path_ontrack_ema_staged, "combined_ema_data.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D1_all_delivered.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D2_per_study_design.RData"),
    file.path(path_ontrack_ema_staged, "combined_online_puffmarker_episode_data.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D3_random_only.RData")
    ),
  outputs = c(
    file.path(path_ontrack_ema_staged, "codebook.RData"),
    file.path(path_ontrack_ema_staged, "masterlist_final.RData"),
    file.path(path_ontrack_ema_staged, "combined_ema_data_final.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D1_all_delivered_final.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D2_per_study_design_final.RData"),
    file.path(path_ontrack_ema_staged, "combined_online_puffmarker_episode_data_final.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D3_random_only_final.RData")
    )
)

create_datasets_with_integers <- list(
  file.name = "ema-scripts/create-ema-datasets-with-integers.R",
  domain = "EMA",
  inputs = c(
    file.path(path_ontrack_ema_staged, "all_ema_data_D2_per_study_design_final.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D3_random_only_final.RData"),
    file = file.path(path_ontrack_ema_staged, "codebook.RData")
    ),
  outputs = c(
    file.path(path_ontrack_ema_staged, "all_ema_data_D2_per_study_design_integers.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D3_random_only_integers.RData"),
    file.path(path_ontrack_ema_staged, "create_and_apply_value_labels_SAS_script.RData")
    )
)

output_formatted_ema_data <- list(
  file.name = "other-scripts/output-formatted-database.R",
  domain = "EMA",
  inputs = c(
    file.path(path_ontrack_ema_staged, "masterlist_final.RData"),
    file.path(path_ontrack_ema_staged, "combined_ema_data_final.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D1_all_delivered_final.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D2_per_study_design_final.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D3_random_only_final.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D2_per_study_design_integers.RData"),
    file.path(path_ontrack_ema_staged, "all_ema_data_D3_random_only_integers.RData"),
    file.path(path_ontrack_ema_staged, "combined_online_puffmarker_episode_data_final.RData"),
    file.path(path_ontrack_ema_staged, "codebook.RData")
  ),
  outputs = c(
    file.path(path_ontrack_ema_output_4_analysis, "masterlist.rds"),
    file.path(path_ontrack_ema_output_4_analysis, "masterlist.dta"),
    file.path(path_ontrack_ema_output_4_analysis, "masterlist.csv"),
    file.path(path_ontrack_ema_output_dm, "ema_items_all.csv"),
    file.path(path_ontrack_ema_output_dm, "all_ema_data-1-all_delivered.rds"),
    file.path(path_ontrack_ema_output_dm, "all_ema_data-1-all_delivered.dta"),
    file.path(path_ontrack_ema_output_dm, "all_ema_data-1-all_delivered.csv"),
    file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-2-per_study_design.rds"),
    file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-2-per_study_design.dta"),
    file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-2-per_study_design.csv"),
    file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-2-per_study_design_integers.rds"),
    file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-2-per_study_design_integers.dta"),
    file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-2-per_study_design_integers.csv"),
    file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-3-random_only_ema.rds"),
    file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-3-random_only_ema.dta"),
    file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-3-random_only_ema.csv"),
    file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-3-random_only_ema_integers.rds"),
    file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-3-random_only_ema_integers.dta"),
    file.path(path_ontrack_ema_output_4_analysis, "all_ema_data-3-random_only_ema_integers.csv"),
    file.path(path_ontrack_ema_output_4_analysis, "online_puffmarker_episode_data.rds"),
    file.path(path_ontrack_ema_output_4_analysis, "online_puffmarker_episode_data.dta"),
    file.path(path_ontrack_ema_output_4_analysis, "online_puffmarker_episode_data.csv"),
    file.path(path_ontrack_ema_output_4_analysis, "codebook.rds"),
    file.path(path_ontrack_ema_output_4_analysis, "codebook.dta"),
    file.path(path_ontrack_ema_output_4_analysis, "codebook.xlsx"),
    file.path(path_ontrack_ema_output_dm, "log.txt")
  )
)

df_dependencies <- as.data.frame(rbind(read_ema_cc1, parse_ema_quest_cc1, gen_ema_dat_cc1, read_ema_cc2, 
                                       parse_ema_quest_cc2, gen_ema_dat_cc2, ema_first_last_dates, 
                                       compile_mda_sas_files, mda_visit_data_prep, utah_visit_data_prep, 
                                       combine_dat_masters, read_battery_data, process_battery_data,
                                       calculate_study_day_ema_blocks, 
                                       generate_block_level_dataset_cc1, generate_block_level_dataset_cc2,
                                       biomarker_read_raw_cc1, biomarker_gen_dat_cc1,
                                       biomarker_read_raw_cc2, biomarker_gen_dat_cc2,
                                       combine_cc1_cc2, create_recalc_ema_vars, create_ema_per_study_design,
                                       create_random_only_ema, create_codebook, create_datasets_with_integers, output_formatted_ema_data))


remove(list = setdiff(ls(), c("df_dependencies", vars_pre)))
