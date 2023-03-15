# Use similar to the "data-curation-pipeline.R" file from Break Free,
# except by specifying dependencies, it will run through all 
# .R and .Rmd files from top to bottom executing necessary 
# files as necessary
options(show.error.locations = TRUE)

remove(list = ls())

library(stringr)

# ----------------------------------------------------------
# Define our error-handling pause function:
pause=function(){
  on.exit(eat_input())
  cat("Paused on error\n")
  cat("(any subsequent code will be ignored)\n")
  cat(traceback())
  Sys.sleep(Inf)
}

# Define our 'eat_input' function to prevent output of subsequent, not-run input:
eat_input=function(){
  cat("\033[1A")
  while((x=readline())!='')cat("\033[1A")
}

# Set the 'error' option to execute our pause function:
options(error=pause)
# --------------------------------------------------------
source("dependencies.R")

# # -------------------------------------------------------
# # Format example
# # -------------------------------------------------------
# # File Name (.R or .rmd)
# # Inputs (list all)
# # Outputs (list all)
# 

df_dependencies <- df_dependencies %>% mutate(ran_file = NA)

if(F){ # initialize parameters for testing function
  i <- 1
  row_i <- df_dependencies[i,]
  }

# define function to be used in for loop across the rows of df_dependencies
dependencies_updated_yn <- function(row_i){
  # Function to determine if the dependencies for the given files outputs have been updated later than the outputs
  # Rule #1: if the inputs are newer than the main file or the outputs, then need to run that main file
  # Rule #2: if the main file is newer than the outputs, then need to run that main file.
  # Innate hierarchy means that if either any input or the main file are newer than the outputs,
  # then the main file needs to be run
  
  # Step 1. get the date of the most recently updated file out of the inputs and main file
  files_input_main_names <- na.omit(unlist(c(unlist(row_i$inputs), row_i$file.name)))
  files_input_main_lastedits <- unlist(lapply(files_input_main_names, function(x) tail(fs::file_info(x)$modification_time)))
  files_input_main_lastedit_newest <- max(files_input_main_lastedits)
  
  # Step 2. get the date of the latest update to the outputs
  files_output_names <- unlist(row_i$outputs)
  files_output_lastedits <- unlist(lapply(files_output_names, function(x) tail(fs::file_info(x)$modification_time)))
  files_output_lastedit_oldest <- min(files_output_lastedits)
  
  need_to_run_main <- files_input_main_lastedit_newest > files_output_lastedit_oldest |  # the outputs are older than the inputs or the main
                        is.na(files_output_lastedit_oldest) # OR the output file does not exist
  return(need_to_run_main)
}

# Variable with the names of the environment variables before sourcing any of the data curation files
vars_pre_loop <- ls()

## Add more to capture errors and warnings
## Add more to log what files ran
for(i_df_dependencies in 1:nrow(df_dependencies)){ # using obscure name i_df_dependencies instead of i, because i is probably used in loops in the sourced scripts
  row_i <- df_dependencies[i_df_dependencies,]  # Each pass through the loop, take one row at a time
  if(dependencies_updated_yn(row_i) %in% c(T, NA)){ # function determines if any dependencies are newer than the respective file's output (dependencies include the input and the generating file)
    message(paste0("Starting to execute file: ", row_i$file.name))
    df_dependencies$ran_file[i_df_dependencies] <- TRUE
    if (str_split(row_i$file.name, pattern = "\\.", simplify = T)[2] == "Rmd"){  # if the respective file name is a .Rmd file
      rmarkdown::render(as.character(row_i$file.name))
      message(paste0("Executed file: ", row_i$file.name))
    } else if (str_split(row_i$file.name, pattern = "\\.", simplify = T)[2] == "R"){ # else if the respective file name is a .R file
      source(as.character(row_i$file.name))
      message(paste0("Executed file: ", row_i$file.name))
    } else { # Catches non .Rmd and .R files
      message("Could not execute the file. Not .R or .Rmd")
    }
    remove(list = setdiff(ls(), c(vars_pre_loop, "i_df_dependencies", "vars_pre_loop"))) # remove variables only from the sourced file
  } else {
    df_dependencies$ran_file[i_df_dependencies] <- FALSE
    message(paste0("No dependencies updated / No need to execute file: ", row_i$file.name))
  }
}

df_dependencies %>% select(file.name, ran_file)
# traceback()
options(error=NULL)

