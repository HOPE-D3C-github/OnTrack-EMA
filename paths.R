library(dplyr)
library(stringr)

development <- FALSE

pre_box <- getwd() %>% str_remove("OneDrive.+")

# Inputs from various folders
path_ontrack_mda_raw <- paste0(pre_box, "Box/OnTrack Data - MD Anderson - Raw/OnTrack_MDA_Tables_2022-09-02")
path_ontrack_cc1_input_data <- paste0(pre_box, "Box/MD2K Utah/CC1_Export")
path_ontrack_cc2_input_data <- paste0(pre_box, "Box/MD2K Utah/EMAs")

# Inputs folder
path_ontrack_visit_inputs <- paste0(pre_box, "Box/OnTrack Pre-Curated Data/Visit Outcomes Data/Inputs")
path_ontrack_ema_inputs <- paste0(pre_box, "Box/OnTrack Pre-Curated Data/EMA Data/inputs")
path_ontrack_quest_inputs <- paste0(pre_box, "Box/OnTrack Pre-Curated Data/Questionnaire Data/inputs")

# Staged folder
path_ontrack_visit_staged <- paste0(pre_box, "Box/OnTrack Pre-Curated Data/Visit Outcomes Data/Staged")
path_ontrack_ema_staged <- paste0(pre_box, "Box/OnTrack Pre-Curated Data/EMA Data/staged")

# Outputs folder
path_ontrack_visit_outputs <- paste0(pre_box, "Box/OnTrack Pre-Curated Data/Visit Outcomes Data/Outputs")
if(development){
  path_ontrack_ema_output_dm <- paste0(pre_box, "Box/OnTrack Pre-Curated Data/EMA Data/outputs - data managers only (temp location)")
  path_ontrack_ema_output_4_analysis <- paste0(pre_box, "Box/OnTrack Pre-Curated Data/EMA Data/outputs - data for analysis (temp location)")
} else {
  path_ontrack_ema_output_dm <- paste0(pre_box, "Box/OnTrack Curated Data - available upon request/EMA/Release v1.0/output - 4 data managers")
  path_ontrack_ema_output_4_analysis <- paste0(pre_box, "Box/OnTrack Curated Data/EMA/Release v1.0/data for analysis")
}

path_ontrack_quest_output_4_analysis <- paste0(pre_box, "Box/OnTrack Pre-Curated Data/Questionnaire Data/outputs - data managers only (temp location)")
