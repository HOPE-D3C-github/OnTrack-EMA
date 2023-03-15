# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary:  Read in raw MD2K CC2 biomarker data files and compile into a list
#             of participant level dataframes
#     
# Inputs:   Raw MD2K CC2 data files, organized in folders per participant
#     
# Outputs:  file.path(path_ontrack_ema_staged, "online_puffmarker_episode_raw_data_cc2.RData")
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(dplyr)
source("paths.R")

# Participant IDs for data collected using CC2 platform
ids_cc2 <- list.files(path = path_ontrack_cc2_input_data)

# -----------------------------------------------------------------------------
# Read raw data: Smoking episodes
# -----------------------------------------------------------------------------
this_string <- "PUFFMARKER_SMOKING_EPISODE--org.md2k.streamprocessor--PHONE.csv.bz2"

list_df_raw <- list()

for(i in 1:length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_ontrack_cc2_input_data, this_id))
  # Pick out file names related to data stream of interest
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Check whether file exists for this given participant
  if(!is.na(this_file)){
    df_raw <- read.csv(file.path(path_ontrack_cc2_input_data, 
                                 this_id, 
                                 this_file), 
                       header = FALSE)
    # Add column to record participant ID
    df_raw <- df_raw %>% mutate(participant_id = this_id) %>% select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    # Add df_raw to collection
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    next
  }
}

online_puffmarker_episode_files_cc2 <- list_df_raw

# -----------------------------------------------------------------------------
# Save RData files
# -----------------------------------------------------------------------------
save(online_puffmarker_episode_files_cc2,
     file = file.path(path_ontrack_ema_staged, "online_puffmarker_episode_raw_data_cc2.RData"))
