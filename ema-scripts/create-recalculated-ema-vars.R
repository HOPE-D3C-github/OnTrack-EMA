# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary:    Re-calculate fields and calculate new variables in the EMA dataset
#     
# Inputs:     file.path(path_ontrack_ema_staged, "combined_ema_data.RData")
#     
# Outputs:    file.path(path_ontrack_ema_staged, "all_ema_data_D1_all_delivered.RData")
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(dplyr)
library(lubridate)
library(tidyr)
library(stringr)

source("paths.R")

load(file = file.path(path_ontrack_ema_staged, "combined_ema_data.RData"))
all_ema_data <- all_ema_data %>% arrange(ID_enrolled, study_day_int, block, end_hrts_local)
dat_master <- readRDS(file.path(path_ontrack_ema_staged, "masterlist_updated.RDS"))
##################################################################
# Update the undelivered EMAs so they have with_any_response = 0
##################################################################
all_ema_data <- all_ema_data %>% mutate(with_any_response = if_else(is.na(with_any_response), F, with_any_response))

##################################################################
### Recalculate timing of EMA vars and flag any discrepancies
##################################################################
#calculate time since last completed EMA - subset to completed EMAs
data2_cpl <- all_ema_data %>% 
  #we only want to calculate lag on completed EMAs
  filter(with_any_response==1) %>%
  arrange(ID_enrolled, end_unixts) %>% 
  group_by(ID_enrolled) %>%
  mutate(end_hrts_last = lag(end_hrts_UTC),
         minutes_since_last = interval(end_hrts_last,end_hrts_UTC) %/% minutes(1),  #floors minutes, or at least rounds
         hours_since_last = minutes_since_last/60) %>%
  ungroup

data2_not_cpl <- all_ema_data %>% filter(with_any_response!=1)
data2 <- bind_rows(data2_cpl,data2_not_cpl) %>% 
  arrange(ID_enrolled, end_unixts)

if(nrow(data2) != nrow(all_ema_data)){print("Error in determining last EMA")}

#supress warnings
default_warning_setting <- getOption("warn")
options(warn = -1)

data3 <- data2 %>%
  #convert hm to minutes -- only for non-cigarette tobaccos because 
  mutate(across(c(othertob_cgr_ago,othertob_cgr_first,othertob_cgr_recent,othertob_ecig_first,othertob_ecig_recent,othertob_marij_first,othertob_marij_recent),
                ~ period_to_seconds(lubridate::hms(.))/60),
  ) %>% 
  #the rest of the cig sequence is calculated only for cigs because the questions are asked with interval categorical answers
  mutate(cig_ago_beg = cig_ago,
         cig_first_beg = cig_first,
         cig_recent_beg = cig_recent) %>% 
  #create begin  
  mutate(across(c(cig_ago_beg,cig_first_beg,cig_recent_beg),
                ~recode(.,
                        "0 - 2 hrs"=0,
                        "2 hrs - 4 hrs"=2,
                        "4 hrs - 6 hrs"=4,
                        "6 hrs - 8 hrs"=6,
                        "8 hrs - 10 hrs"=8,
                        "10 hrs - 12 hrs"=10,
                        "More than 12 hrs"=12))) %>% 
  #create temp times (cig and other) -- this mutate across just creates copies of the variable with _temp_ at the end of the name
  mutate(across(c(cig_first_beg,cig_recent_beg,
                  cig_first,cig_recent,
                  othertob_cgr_first, othertob_cgr_recent,
                  othertob_ecig_first, othertob_ecig_recent,
                  othertob_marij_first,othertob_marij_recent), 
                .names = "{.col}_temp_", 
                ~ (.)))   #notice the function doesn't do anything

#turn off warning supression
options(warn = default_warning_setting)

data4 <- data3 %>% 
  #add indicator if flipped
  mutate(cig_flip = replace_na(cig_first_beg<cig_recent_beg,F),
         othertob_cgr_flip = replace_na(othertob_cgr_first<othertob_cgr_recent,F),
         othertob_ecig_flip = replace_na(othertob_ecig_first<othertob_ecig_recent,F),
         othertob_marij_flip = replace_na(othertob_marij_first<othertob_marij_recent,F)) %>% 
  #flip times
  mutate(cig_first_beg = if_else(cig_flip,cig_recent_beg_temp_,cig_first_beg_temp_),
         cig_recent_beg = if_else(cig_flip, cig_first_beg_temp_, cig_recent_beg_temp_),
         cig_first = if_else(cig_flip,cig_recent_temp_,cig_first_temp_),
         cig_recent = if_else(cig_flip,cig_first_temp_,cig_recent_temp_),
         othertob_cgr_first = if_else(othertob_cgr_flip,othertob_cgr_recent_temp_,othertob_cgr_first_temp_),
         othertob_cgr_recent = if_else(othertob_cgr_flip,othertob_cgr_first_temp_,othertob_cgr_recent_temp_),
         othertob_ecig_first = if_else(othertob_ecig_flip,othertob_ecig_recent_temp_,othertob_ecig_first_temp_),
         othertob_ecig_recent = if_else(othertob_ecig_flip,othertob_ecig_first_temp_,othertob_ecig_recent_temp_),
         othertob_marij_first = if_else(othertob_marij_flip,othertob_marij_recent_temp_,othertob_marij_first_temp_),
         othertob_marij_recent = if_else(othertob_marij_flip,othertob_marij_first_temp_,othertob_marij_recent_temp_)
  ) %>% 
  #now that times are flipped, I'll calculate midpoint for intervals, not just beginning times
  #create midpoint -- this mutate adds one to each begining time and saves it as the midpoint variable
  mutate(cig_ago_mid = cig_ago_beg+1,
         cig_first_mid = cig_first_beg+1,
         cig_recent_mid = cig_recent_beg+1)


data5 <- data4 %>% 
  #calculate farthest smoke time from present
  rowwise() %>% #run max across rows instead of columns
  mutate(cig_max = max(cig_first_beg,cig_recent_beg,cig_ago_beg, na.rm=T),
         othertob_cgr_max = max(othertob_cgr_first,othertob_cgr_recent,othertob_cgr_ago, na.rm=T),
         othertob_ecig_max = max(othertob_ecig_first,othertob_ecig_recent, na.rm=T),
         othertob_marij_max = max(othertob_marij_first,othertob_marij_recent, na.rm=T)) %>% 
  ungroup %>% 
  #see if that farthest time happens before last EMA -- since most recent non-missing EMA
  mutate(
    cig_err = cig_max>hours_since_last,
    othertob_cgr_err = othertob_cgr_max>minutes_since_last,
    othertob_ecig_err = othertob_ecig_max>minutes_since_last,
    othertob_marij_err = othertob_marij_max>minutes_since_last
  )

if(F){data5 %>% filter(cig_err) %>% select(cig_first_beg,cig_recent_beg,cig_ago_beg, cig_max, hours_since_last)
data5 %>% filter(!cig_err) %>% select(cig_first_beg,cig_recent_beg,cig_ago_beg, cig_max, hours_since_last)
data5 %>% filter(othertob_marij_err) %>% select(othertob_marij_first,othertob_marij_recent,othertob_marij_max, minutes_since_last)
data5 %>% filter(!othertob_marij_err) %>% select(cig_first_beg,cig_recent_beg,cig_ago_beg, cig_max, hours_since_last)}

newvars <- setdiff(names(data5),names(all_ema_data))
toremove <- c(str_subset(newvars,"_temp_$"),str_subset(newvars,"_max$"),str_subset(newvars,"_beg$"))
data6 <- data5 %>% 
  select(-all_of(toremove))

##################################################################
### explore issues with Skip Logic
##################################################################
library(readr)
library(dplyr)
library(stringr)
library(purrr)
library(tidyr)

#custom operands for use in evaluation of conditions
'%detect%' = function(alpha,bravo){str_detect(alpha,fixed(bravo))}
'%no_detect%' = function(alpha,bravo){!str_detect(alpha,fixed(bravo))}

data_prefix <- data6

conditions <- ema_items_labelled %>% 
  filter(!is.na(condition)) %>% 
  mutate(condition = case_when(is.na(condition) ~ NA_character_,
                               str_detect(condition,"^\\(")&str_detect(condition,"\\)\\|\\(") ~  str_replace(str_replace(condition,"\\(","\\(data_prefix$"),"\\)\\|\\(","\\)\\|\\(data_prefix$"),
                               T ~ str_c("data_prefix$",condition)),
         condition_inv = str_replace_all(condition,"= ","== ") %>% str_replace_all(.,"===","!=") %>% str_replace_all(.,"!==","=="),
         condition_inv = str_replace_all(condition_inv,"detect%","no_detect%") %>% str_remove_all(.,"no_no_"),
         condition_violate = str_c("(",condition_inv,")&!is.na(data_prefix$",varname_desc,")"))

conditions_applied = conditions %>% 
  mutate(
    condition_applied_table = map(condition, ~table(eval(str2lang(.)), useNA = "ifany")),
    condition_applied_data = map(condition, ~ subset(data_prefix,eval(str2lang(.))) %>% as_tibble),
    condition_applied_nrow = map(condition_applied_data, nrow) %>% as.numeric,
    condition_viol_applied_data = map(condition_violate, ~ subset(data_prefix,eval(str2lang(.))) %>% as_tibble),
    condition_viol_applied_nrow = map(condition_viol_applied_data, nrow) %>% as.numeric
  )

conditions_applied_simple <- conditions_applied %>% select(varname_desc,condition,condition_applied_nrow, condition_violate, condition_viol_applied_nrow) %>% as_tibble

#because people with 1 reported cig should only be given `cig_ago`, I am moving over timing from cig_recent.
all_ema_data_fixed <- data_prefix %>% 
  mutate(cig_ago = if_else(replace_na(cig_n_v1==1 & !is.na(cig_recent),F),"0 - 2 hrs",cig_ago))

unedited_and_clean_ema_vars_dat <- all_ema_data_fixed %>% select(ID_enrolled, cc_indicator) #Add first components, then will grow as the for loop iterates below

for (myq_id in conditions$varname_breakfree){
  
  unedited_and_clean_ema_vars_dat <- unedited_and_clean_ema_vars_dat %>%
    mutate(placeholder_name_1 = all_ema_data_fixed[[myq_id]])
  
  names(unedited_and_clean_ema_vars_dat)[names(unedited_and_clean_ema_vars_dat) == "placeholder_name_1"] <- paste0(myq_id, "_unedited")
  
  my_condition_violate <- conditions %>% filter(varname_breakfree==myq_id) %>% .$condition_violate
  violated <- replace_na(eval(str2lang(my_condition_violate)),F)
  if(is.numeric(all_ema_data_fixed[[myq_id]])) {all_ema_data_fixed[violated,myq_id] <- NA_real_}
  else if (is.character(all_ema_data_fixed[[myq_id]])) {all_ema_data_fixed[violated,myq_id] <- NA_character_}
  else {print("ERROR: unexpexted vartype"); break}
  
  print(paste0("For variable (",myq_id,"), we determine who meet this criteria: ",my_condition_violate))
  print(paste0(sum(replace_na(violated,F))," were updated to have NA values"))
  
  unedited_and_clean_ema_vars_dat <- unedited_and_clean_ema_vars_dat %>%
    mutate(placeholder_name_2 = all_ema_data_fixed[[myq_id]])
  names(unedited_and_clean_ema_vars_dat)[names(unedited_and_clean_ema_vars_dat) == "placeholder_name_2"] <- paste0(myq_id, "_clean")
}

all_ema_data_cleaned <- all_ema_data_fixed

# START Updates for any_tob_use variable ####
# Right now, it includes e-cigarettes and vaporizers - can remove from below if desired (contains nicotine but not tobacco)
all_ema_data_cleaned <-  all_ema_data_cleaned %>% mutate(
  any_tob_use = case_when(
    cig_yn == "Yes"  ~ TRUE,
    str_detect(string = othertob_which_v1, pattern = "Cigars, cigarillos, or little cigars|Chew, snuff, or dip|Vaporizer or e-cigarettes|Hookah or waterpipe|Pipe will with tobacco \\(not waterpipe\\)|Other tobacco product") ~ TRUE,
    str_detect(string = othertob_which_v2, pattern = "Cigars, cigarillos, or little cigars|Vape pen, JUUL or e-cigarettes") ~ TRUE,
    cc_indicator == 1 & is.na(othertob_which_v1) ~ NA,
    cc_indicator == 2 & is.na(othertob_which_v2) ~ NA,
    T ~ FALSE
  )) 
# END update for any_tob_use variable

# Start multi-hot encoding for each "othertob_which" option
all_ema_data_cleaned <- all_ema_data_cleaned %>% mutate(
  cigars_cigarillos_yn = case_when(cc_indicator == 1 & str_detect(string = othertob_which_v1, pattern = "Cigars, cigarillos, or little cigars") ~ TRUE,
                                   cc_indicator == 2 & str_detect(string = othertob_which_v2, pattern = "Cigars, cigarillos, or little cigars") ~ TRUE,
                                   cc_indicator == 1 & is.na(othertob_which_v1) ~ NA,
                                   cc_indicator == 2 & is.na(othertob_which_v2) ~ NA,
                                   T~FALSE),
  chew_snuff_dip_yn = case_when(cc_indicator == 1 & str_detect(string = othertob_which_v1, pattern = "Chew, snuff, or dip") ~ TRUE,
                                cc_indicator == 1 & is.na(othertob_which_v1) ~ NA,
                                cc_indicator == 2 & is.na(othertob_which_v2) ~ NA,
                                T~FALSE),
  vape_ecig_yn = case_when(cc_indicator == 1 & str_detect(string = othertob_which_v1, pattern = "Vaporizer or e-cigarettes") ~ TRUE,
                           cc_indicator == 2 & str_detect(string = othertob_which_v2, pattern = "Vape pen, JUUL or e-cigarettes") ~ TRUE,
                           cc_indicator == 1 & is.na(othertob_which_v1) ~ NA,
                           cc_indicator == 2 & is.na(othertob_which_v2) ~ NA,
                           T~FALSE),
  hookah_waterpipe_yn = case_when(cc_indicator == 1 & str_detect(string = othertob_which_v1, pattern = "Hookah or waterpipe") ~ TRUE,
                                  cc_indicator == 1 & is.na(othertob_which_v1) ~ NA,
                                  cc_indicator == 2 & is.na(othertob_which_v2) ~ NA,
                                  T~FALSE),
  tob_pipe_notwaterpipe_yn = case_when(cc_indicator == 1 & str_detect(string = othertob_which_v1, pattern = "Pipe will with tobacco \\(not waterpipe\\)") ~ TRUE, #Pipe will with tobacco (not waterpipe)
                                       cc_indicator == 1 & is.na(othertob_which_v1) ~ NA,
                                       cc_indicator == 2 & is.na(othertob_which_v2) ~ NA,
                                       T~FALSE),
  other_tobacco_yn = case_when(cc_indicator == 1 & str_detect(string = othertob_which_v1, pattern = "Other tobacco product") ~ TRUE,
                               cc_indicator == 1 & is.na(othertob_which_v1) ~ NA,
                               cc_indicator == 2 & is.na(othertob_which_v2) ~ NA,
                               T~FALSE),
  marijuana_inhaled_yn = case_when(cc_indicator == 2 & str_detect(string = othertob_which_v2, pattern = "Marijuana/Cannabis \\(inhaled/vaped/smoked\\)") ~ TRUE,
                                   cc_indicator == 1 & is.na(othertob_which_v1) ~ NA,
                                   cc_indicator == 2 & is.na(othertob_which_v2) ~ NA,
                                   T~FALSE),
  marijuana_edible_yn = case_when(cc_indicator == 2 & str_detect(string = othertob_which_v2, pattern = "Marijuana/Cannabis \\(inhaled/vaped/smoked\\)") ~ TRUE,
                                  cc_indicator == 1 & is.na(othertob_which_v1) ~ NA,
                                  cc_indicator == 2 & is.na(othertob_which_v2) ~ NA,
                                  T~FALSE))

# ----------------------------------------------------------------
# Ontrack updates to match breakfree setup for indicators
all_ema_data_cleaned <- all_ema_data_cleaned %>% rename(multi_day_start = extra_day_start)

all_ema_data_cleaned <- all_ema_data_cleaned %>% 
  mutate(multi_day_start_on_study_day = multi_day_start) %>% 
  mutate(
    multi_day_start = case_when(
      !multi_day_start_on_study_day ~ FALSE,
      !ema_delivered & day_start_hrts_local_2 < block_end_hrts_local ~ TRUE,       # ema was not delivered and end of block was after the 2nd day start
      !ema_delivered & day_start_hrts_local_2 >= block_end_hrts_local ~ FALSE,     # ema was not delivered and end of block was before the 2nd day start
      ema_delivered & day_start_hrts_local_2 < end_hrts_local ~ TRUE,              # ema was delivered and the end timestamp occurred after the 2nd day start
      ema_delivered & day_start_hrts_local_2 >= end_hrts_local ~ FALSE,            # ema was delivered and the end timestamp occurred before the 2nd day start
      T ~ NA
    ) )

all_ema_data_cleaned <- all_ema_data_cleaned %>% 
  mutate(invalid_end_day_on_study_day = any(invalid_end_day), .by = c(ID_enrolled, study_day_int))

all_ema_data_cleaned <- all_ema_data_cleaned %>% select(-participant_id, -end_of_day_before_block_start, -end_of_day_during_block, -withdrew_date, 
                                                        -day_end_hrts_local, -day_start_unixts, -day_end_unixts, -day_start_unixts_2, -day_start_hrts_local_2, 
                                                        -day_end_unixts_2, -day_end_hrts_local_2, -lead_day_start_hrts_local, -ema_delivered_hrts_local, 
                                                        -ema_delivered_hrts_local_2, -first_day_date, -last_day_date, -extra_ema_on_block, -no_day_start_on_study_day, -after_pt_withdrew)
# ----------------------------------------------------------------
# Reposition columns
all_ema_data_D1_all_delivered <- all_ema_data_cleaned %>% 
  relocate(ema_delivered, begin_hrts_UTC, end_hrts_UTC, day_start_hrts_local,
           block_start_hrts_local, block_end_hrts_local, extra_ema,
           extra_ema_on_study_day, multi_day_start, multi_day_start_on_study_day,
           invalid_end_day, invalid_end_day_on_study_day, .after = everything()) %>% 
  relocate(cc_indicator, study_date, study_day_int, block, .after = ID_enrolled) %>% 
  relocate(with_any_response, .after = ema_type) %>% 
  relocate(timezone_local, begin_hrts_local, end_hrts_local, .before = ema_type)

# ---------------------------------------------------------------
# Update undelivered reason for participants without any EMA data
# ---------------------------------------------------------------
all_ema_data_D1_all_delivered <- all_ema_data_D1_all_delivered %>% 
  left_join(dat_master %>% select(ID_enrolled, in_ematimes),
            by = "ID_enrolled") %>% 
  mutate(status = case_when(
    !in_ematimes ~ "UNDELIVERED-PT_NO_EMA_ENTIRE_STUDY",
    T ~ status
  )) %>% 
  select(-in_ematimes)

# ---------------------------------------------------------------
# Save data
# ---------------------------------------------------------------
save(all_ema_data_D1_all_delivered,conditions_applied_simple, unedited_and_clean_ema_vars_dat,
     file = file.path(path_ontrack_ema_staged, "all_ema_data_D1_all_delivered.RData"))

