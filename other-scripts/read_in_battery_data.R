# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary:  Read in each participant's phone battery data, apply initial 
#             data reduction filters, and create a list of participant-level 
#             battery data dataframes           
#     
# Inputs:   file.path(path_ontrack_visit_outputs, "masterlist.RData")
#     
# Outputs:  file.path(path_ontrack_ema_staged, "filtered_battery_data.RData")
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(dplyr)
library(lubridate)
library(tidyr)
library(tictoc)

source("paths.R")
source(file.path("collect-functions", "io-utils.R"))

tic("Main")

load(file = file.path(path_ontrack_visit_outputs, "masterlist.RData"))

# -----------------------------------------------------------------------------
# Starting with CC1 ####
# "...+BATTERY+PHONE.json" contains metadata
# "...+BATTERY+PHONE.csv" is a zipped folder containing a csv of battery data
# -----------------------------------------------------------------------------

# Participant IDs for data collected using CC1 platform
ids_cc1 <- append(dat_master %>% filter(cc_indicator == 1) %>% .$participant_id,
                  dat_master %>% filter(cc_indicator == 1 & !is.na(participant_id_2)) %>% .$participant_id_2
                  )

# -----------------------------------------------------------------------------
# Check for existence of files and determine whether each participant has a unique copy of each file ####
# -----------------------------------------------------------------------------
dat_zipped_file_counts_cc1 <- data.frame(participant_id = ids_cc1,
                                         count_battery_zip_csv_file = NA)

for(i in 1:length(ids_cc1)){
  this_id <- ids_cc1[i]
  
  n_battery_zip_csv <- CountFile(participant_id = this_id, 
                                 file_name = "BATTERY+PHONE.csv.zip", 
                                 directory = path_ontrack_cc1_input_data)
  
  dat_zipped_file_counts_cc1 <- dat_zipped_file_counts_cc1 %>%
    mutate(count_battery_zip_csv_file = replace(count_battery_zip_csv_file, participant_id == this_id, n_battery_zip_csv))
}

# Calculate summary statistics
# If maximum number displayed is 1, then there are no duplicate files
dat_zipped_file_counts_cc1 %>%
  summarise(max_zip_csv = max(count_battery_zip_csv_file),
            min_zip_csv = min(count_battery_zip_csv_file))


# ----------------------------------------------------------------------------- 
# Check for existence of more than one csv in each participants zipped battery folder ####
# -----------------------------------------------------------------------------
if (T){
  dat_csv_file_counts_cc1 <- data.frame(participant_id = ids_cc1,
                                        count_battery_csv_file = NA)
  
  this_string <- "BATTERY+PHONE.csv"
  
  for(i in 1:length(ids_cc1)){
    this_id <- ids_cc1[i]
    
    # List all file names within folder corresponding to this_id
    all_files <- list.files(file.path(path_ontrack_cc1_input_data, this_id))
    # Pick out file names related to data stream of interest
    idx <- grepl(pattern = this_string, 
                 x = all_files, 
                 fixed = TRUE)
    # Pick out corresponding files
    this_file <- all_files[idx]
    is_any_file <- length(this_file)
    
    if(is_any_file == 1){
      unzipped_all <- unzip(zipfile = file.path(path_ontrack_cc1_input_data, this_id, this_file), list = TRUE)
      unzipped_filenames <- unzipped_all$Name
      n_battery_csv <- length(unzipped_filenames)
      
      dat_csv_file_counts_cc1 <- dat_csv_file_counts_cc1 %>%
        mutate(count_battery_csv_file = replace(count_battery_csv_file, participant_id == this_id, n_battery_csv))
    }
  }
  
  dat_csv_file_counts_cc1 %>%
    summarise(max_zip_csv = max(count_battery_csv_file),
              min_zip_csv = min(count_battery_csv_file))}


# -----------------------------------------------------------------------------
# Read CC1 battery raw data ####
# -----------------------------------------------------------------------------
list_df_filtered <- list()

# Specify data stream of interest
this_string <- "BATTERY+PHONE.csv"
tic(msg = "CC1 battery data")
#start_time <- Sys.time()
for(i in 1: length(ids_cc1)){
  this_id <- ids_cc1[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_ontrack_cc1_input_data, this_id))
  # Pick out file names related to data stream of interest
  idx <- grepl(pattern = this_string, 
               x = all_files, 
               fixed = TRUE)
  # Pick out corresponding files
  this_file <- all_files[idx]
  is_any_file <- length(this_file)
  
  if(is_any_file == 1){
    unzipped_all <- unzip(zipfile = file.path(path_ontrack_cc1_input_data, this_id, this_file), list = TRUE)
    unzipped_filenames <- unzipped_all$Name
    
    tmp <- try(read.csv(unz(file.path(path_ontrack_cc1_input_data, this_id, this_file),
                            unzipped_filenames), sep = ",", header = FALSE))
    if (!inherits(tmp, 'try-error')){ 
      # Ran into an error when there are no lines of data in the file ""no lines available in input". this logical allows the process to continue
      df_raw <- tmp
      remove(tmp)
      
      # Seeing an issue where other data is recorded in the battery log. Appears to be phone status data. Writing to a V6 column only when the occurs
      if(ncol(df_raw) == 6){
        # Remove the rows with 6 columns of data (sixth column is not '') and then select the first 5 rows
        df_raw <- df_raw %>% filter(V6 == '') %>% select(colnames(.)[1:5])
      }
      # Seeing another issue where phone status data is written in with the battery data. Identifiable by:
      # 2) Has empty value '' for 3rd column (which is supposed to be battery percent)
      df_raw <- df_raw %>% filter(V3 != '' )
      
      if (ncol(df_raw) == 5){   
        # Another issue where the data file didn't have all the 5 columns (no headers in the csv)
        df_raw <- df_raw %>%  
          stats::setNames(c("datetime", "unk_1", "battery_percent", "battery_voltage", "unk_2")) %>% 
          select(datetime, battery_percent)
        
        df_raw  <- df_raw %>% mutate(datetime = datetime/1000)  # convert from unix with milliseconds to unix with seconds
        
        df_filtered <- df_raw
        
        # test: group_by date-hour-minute level then take 1st per minute group
        # Add comments about why to subset and how its done
        
        df_filtered <- df_filtered %>% 
          mutate(battery_percent = as.integer(round(as.numeric(battery_percent))),
                 datetime_UTC = as.POSIXct(as.numeric(datetime), tz = "UTC", origin="1970-01-01"),
                 datetime_UTC_roundmin = round_date(datetime_UTC, unit = "minute"),
                 lag_diff_battery_percent = battery_percent - lag(battery_percent, order_by = datetime),
                 lead_diff_battery_percent = lead(battery_percent, order_by = datetime) - battery_percent,
                 lag_battery_percent = lag(battery_percent, order_by = datetime),
                 lead_battery_percent = lead(battery_percent, order_by = datetime),
                 lag_diff_secs = datetime - lag(datetime, order_by = datetime))
        
        df_filtered <- df_filtered %>% filter(datetime > 1) %>% filter(battery_percent <= 100 & battery_percent >= 0) %>% 
          filter(!(((lag_diff_battery_percent < -5 & lead_diff_battery_percent >5) | (lag_diff_battery_percent >5 & lead_diff_battery_percent < -5)) & lag_diff_secs < 90)) %>% 
          select(datetime, datetime_UTC_roundmin, battery_percent)
        
        
        df_filtered <- df_filtered %>% 
          group_by(datetime_UTC_roundmin) %>% 
          filter(row_number() == 1) %>% 
          ungroup() %>% 
          select(datetime, battery_percent)
        
        remove(df_raw)
        
        # Add column to record participant ID
        df_filtered <- df_filtered %>% 
          mutate(participant_id = this_id) %>% 
          select(participant_id, everything())
        
        #deduplicate entirely duplicated rows
        df_filtered <- df_filtered[!duplicated(df_filtered),]
        
        list_df_filtered <- append(list_df_filtered, list(df_filtered))
      }
    } 
    
    
  }else{
    # In this case, the file we are looking for does not exist for this participant
    next
  }
}
toc()
# end_time <- Sys.time()
# end_time - start_time    # Time difference of 10.6 mins                      

list_battery_data_cc1 <- list_df_filtered
remove(list_df_filtered)
remove(df_filtered)

if(T){save(list_battery_data_cc1,
           file = file.path(path_ontrack_ema_staged, "battery_60s_filtered_data_cc1.RData"))}

remove(list_battery_data_cc1, dat_csv_file_counts_cc1, dat_zipped_file_counts_cc1, unzipped_all)


# -----------------------------------------------------------------------------
# END cc1 read-raw-data steps (equivalent to "read-raw-data-cc1.R") ####
# -----------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# START cc2 read-raw-data steps ####
# Files are:
# "BATTERY--org.md2k.phonesensor--PHONE.json" - metadata
# "BATTERY--org.md2k.phonesensor--PHONE.csv.bz2" - battery data
# -----------------------------------------------------------------------------

# Participant IDs for data collected using CC2 platform
# Updated to catch the 2nd participant_id some pts had
ids_cc2 <- append(dat_master %>% filter(cc_indicator == 2) %>% .$participant_id,
                  dat_master %>% filter(cc_indicator == 2 & !is.na(participant_id_2)) %>% .$participant_id_2
                  )

# -----------------------------------------------------------------------------
# Check for existence of files and determine whether each participant has a
# unique copy of each file
# -----------------------------------------------------------------------------
dat_csv_file_counts_cc2 <- data.frame(participant_id = ids_cc2,
                                      count_battery_csv_file = NA)

for(i in 1:length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  n_battery_csv <- CountFile(participant_id = this_id, 
                             file_name = "BATTERY--org.md2k.phonesensor--PHONE.csv.bz2", 
                             directory = path_ontrack_cc2_input_data)
  
  dat_csv_file_counts_cc2 <- dat_csv_file_counts_cc2 %>%
    mutate(count_battery_csv_file = replace(count_battery_csv_file, participant_id == this_id, n_battery_csv))
}

# Calculate summary statistics
# If maximum number displayed is 1, then there are no duplicate files
dat_csv_file_counts_cc2 %>%
  summarise(max_csv = max(count_battery_csv_file),
            min_csv = min(count_battery_csv_file))

# -----------------------------------------------------------------------------
# Read CC2 battery raw data
# -----------------------------------------------------------------------------

list_df_filtered <- list()

# Specify file of interest
this_file <- "BATTERY--org.md2k.phonesensor--PHONE.csv.bz2"

tic("CC2 battery")
#start_time <- Sys.time()
for(i in 1: length(ids_cc2)){
  this_id <- ids_cc2[i]
  
  
  tmp <- try(read.csv(file.path(path_ontrack_cc2_input_data, this_id, this_file), 
                      header = FALSE, 
                      sep = ","))
  if (!inherits(tmp, 'try-error')){ 
    # If it inherits a try-error from attempting to read the battery data file, then move on to the next participant
    # Ran into an error when there are no lines of data in the file ""no lines available in input". this logical allows the process to continue
    df_raw <- tmp
    remove(tmp)
    
    # Seeing an issue where other data is recorded in the battery log. Appears to be phone status data. Writing to a V6 column only when the occurs
    if(ncol(df_raw) == 6){
      # Remove the rows with 6 columns of data (sixth column is not '') and then select the first 5 rows
      df_raw <- df_raw %>% filter(V6 == '') %>% select(colnames(.)[1:5])
    }
    
    if (ncol(df_raw) == 5){   
      # Another issue where the data file didn't have all the 5 columns
      # Seeing another issue where phone status data is written in with the batter data. Identifiable by two ways:
      # 1) 4th column has a value of 'DEBUG'
      #   or
      # 2) Has empty value '' for 3rd column (which is supposed to be battery percent)
      if(ncol(df_raw) > 3){df_raw <- df_raw %>% filter( V4 != 'DEBUG' & V3 != '' )}
      
      df_raw <- df_raw %>%  
        stats::setNames(c("datetime", "unk_1", "battery_percent", "battery_voltage", "unk_2")) %>% 
        select(datetime, battery_percent)
      
      df_raw  <- df_raw %>% mutate(datetime = as.numeric(datetime)/1000L)  # convert from unix with milliseconds to unix with seconds
      
      df_filtered <- df_raw
      
      # test: group_by date-hour-minute level then take 1st per minute group
      # Add comments about why to subset and how its done
      
      df_filtered <- df_filtered %>% 
        mutate( battery_percent = as.integer(round(as.numeric(battery_percent))),
               datetime_UTC = as.POSIXct(as.numeric(datetime), tz = "UTC", origin="1970-01-01"),
               datetime_UTC_roundmin = round_date(datetime_UTC, unit = "minute"),
               lag_diff_battery_percent = battery_percent - lag(battery_percent, order_by = datetime),
               lead_diff_battery_percent = lead(battery_percent, order_by = datetime) - battery_percent,
               lag_battery_percent = lag(battery_percent, order_by = datetime),
               lead_battery_percent = lead(battery_percent, order_by = datetime),
               lag_diff_secs = datetime - lag(datetime, order_by = datetime))
      
      df_filtered <- df_filtered %>% filter(datetime > 1) %>% filter(battery_percent <= 100 & battery_percent >= 0) %>% 
        filter(!(((lag_diff_battery_percent < -5 & lead_diff_battery_percent >5) | (lag_diff_battery_percent >5 & lead_diff_battery_percent < -5)) & lag_diff_secs < 90)) %>% 
        select(datetime, datetime_UTC_roundmin, battery_percent)
      
      
      df_filtered <- df_filtered %>% 
        group_by(datetime_UTC_roundmin) %>% 
        filter(row_number() == 1) %>% 
        ungroup() %>% 
        select(datetime, battery_percent)
      
      remove(df_raw)
      
      # Add column to record participant ID
      df_filtered <- df_filtered %>% 
        mutate(participant_id = this_id) %>% 
        select(participant_id, everything())
      
      #deduplicate entirely duplicated rows
      df_filtered <- df_filtered[!duplicated(df_filtered),]
      
      list_df_filtered <- append(list_df_filtered, list(df_filtered))
    }
  } 
}
toc()
                    

list_battery_data_cc2 <- list_df_filtered
remove(list_df_filtered)
remove(df_filtered)


if(T){save(list_battery_data_cc2,
           file = file.path(path_ontrack_ema_staged, "battery_60s_filtered_data_cc2.RData"))}

remove(dat_csv_file_counts_cc2, list_battery_data_cc2)

# -----------------------------------------------------------------------------
# lists are saved in the staged folder, so re-run from the lines below
# -----------------------------------------------------------------------------

load(file = file.path(path_ontrack_ema_staged, "battery_60s_filtered_data_cc1.RData"))
load(file = file.path(path_ontrack_ema_staged, "battery_60s_filtered_data_cc2.RData"))

# -----------------------------------------------------------------------------
# Combine cc1 and cc2 data and rbind the list into one dataframe ####
# -----------------------------------------------------------------------------
list_all_battery_data <- list()
list_all_battery_data <- append(list_all_battery_data, list_battery_data_cc1)
#remove(list_battery_data_cc1)
list_all_battery_data <- append(list_all_battery_data, list_battery_data_cc2) 
#remove(list_battery_data_cc2)

all_battery_data <- do.call(rbind, list_all_battery_data)
#remove(list_all_battery_data)

all_battery_data <- all_battery_data  %>% 
  mutate(datetime_hrts_UTC = as.POSIXct(datetime, tz = "UTC", origin="1970-01-01"))

filtered_battery_data <- all_battery_data %>%
  group_by(participant_id) %>%
  mutate(lag_diff_secs = datetime - lag(datetime, order_by = datetime),
         lag_diff_battery_percent = battery_percent - lag(battery_percent, order_by = datetime),
         lead_diff_battery_percent = lead(battery_percent, order_by = datetime) - battery_percent,
         lag_battery_percent = lag(battery_percent, order_by = datetime),
         lead_battery_percent = lead(battery_percent, order_by = datetime)) %>% ungroup

# Dropping instances where the battery % decreased by more than 5% from the previous reading, then increased over 5% on the next reading, and the previous time reading was within 90 seconds
filtered_battery_data <- filtered_battery_data %>% filter(!(lag_diff_battery_percent < -5 & lead_diff_battery_percent >5 & lag_diff_secs < 90))

# Run back through to recalculate the lag and lead
filtered_battery_data <- filtered_battery_data %>%
  group_by(participant_id) %>% 
  mutate(lag_diff_secs = datetime - lag(datetime, order_by = datetime),
         lag_diff_battery_percent = battery_percent - lag(battery_percent, order_by = datetime),
         lead_diff_battery_percent = lead(battery_percent, order_by = datetime) - battery_percent,
         lag_battery_percent = lag(battery_percent, order_by = datetime),
         lead_battery_percent = lead(battery_percent, order_by = datetime)) %>% ungroup

if(T){save(filtered_battery_data,
           file = file.path(path_ontrack_ema_staged, "filtered_battery_data.RData"))}
toc()
