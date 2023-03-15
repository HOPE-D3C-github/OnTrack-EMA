# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary:  Read in the raw SAS data files of MDA visit data and save a list of 
#             dataframes (1 dataframe per SAS data file)
#     
# Inputs:   ".sas7bdat" files stored at [path_ontrack_mda_raw]
#     
# Outputs:  file.path(path_ontrack_visit_staged, "list_of_all_sas_data_from_MDA.Rds")
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(haven)
library(dplyr)
library(stringr)
library(data.table)

source("paths.R")

file_names <- list.files(path_ontrack_mda_raw)

list_of_df <- list()
list_names <- c()

for (file in file_names){
  if(F){file <- file_names[1]} # Use to initialize for testing
  file_name_parsed <- str_split(file, pattern = "\\.", simplify = TRUE)
  file_name <- file_name_parsed[,1]
  file_suffix <- file_name_parsed[,2] 
  
  if(file_suffix == "sas7bdat"){
    data <- read_sas(file.path(path_ontrack_mda_raw, file))
    list_of_df <- append(list_of_df, list(data))
    list_names <- append(list_names, file_name)
  }
}
remove(file, name, data)

names(list_of_df) <- list_names
names(list_of_df)


summary_df <- data.frame(index = integer(), name = character(), n_rows = integer(), n_cols = integer())
for (i in 1:length(list_of_df)){
  #print(paste0("Index: ",i, ". Name: ", names(list_of_df[i]), ". Rows: ", dim(list_of_df[[i]])[1], ". Cols: ", dim(list_of_df[[i]])[2]))
  summary_df <- summary_df %>% add_row(index = i, name = names(list_of_df[i]), n_rows = dim(list_of_df[[i]])[1], n_cols = dim(list_of_df[[i]])[2])
}
remove(i)

saveRDS(list_of_df,
        file.path(path_ontrack_visit_staged, "list_of_all_sas_data_from_MDA.Rds"))

