library(openxlsx)
library(tidyr)
library(tibble)
library(fuzzyjoin)
library(readr)
library(haven)
library(janitor)

source("paths.R")
path_ontrack_inputs <- paste0(pre_box, "Box/OnTrack Pre-Curated Data/Inputs")
path_to_questionnaire_crswlk <- paste0(pre_box, "Box/OnTrack Pre-Curated Data/Questionnaire Data/staged/Questionnaire Crosswalk")
path_ontrack_questionnaire_inputs <- paste0(pre_box, "Box/OnTrack Pre-Curated Data/Questionnaire Data/inputs")

mda_codebook_quests <- readRDS(file = file.path(path_ontrack_questionnaire_inputs, "MDA_questions_parsed.RDS"))

# -----------------------------------------------------------------------------
# Prep MDA Questionnaire varnames and question text
# 
# Also started on getting unique values per varname. Could look at that concondence
# after question text investigation
# -----------------------------------------------------------------------------
quest_raw <- read_csv(file.path(path_ontrack_mda_raw, "OnTrack_MDA_Responses_ChoiceText.csv")) 
colnames(quest_raw) = make.names(colnames(quest_raw), unique = T)
quest_raw <- quest_raw %>% select(#-StartDate_orig, -EndDate_orig, -RecordedDate_orig, 
                                  -c("StartDate","EndDate",  "Status",   "IPAddress","Progress", "Duration..in.seconds.", "Finished", "RecordedDate","ResponseId",           
                                     "RecipientLastName","RecipientFirstName","RecipientEmail",    "ExternalReference", "LocationLatitude",  "LocationLongitude", "DistributionChannel",   "UserLanguage",      "SubjectID_",           
                                     "Cohort_", "Visit_4",  "SubjectID_1", "Visit_1",  "Cohort_1", "Initial", "Q610_Id", "Q610_Name", "Q610_Size", "Q610_Type",
                                     "Q612_Id", "Q612_Name", "Q612_Size", "Q612_Type", "Q613_Id", "Q613_Name", "Q613_Size", "Q613_Type", "Q614_Id", "Q614_Name", "Q614_Size", "Q614_Type"))

quest_labels <- quest_raw[1:2,]

quest_dat <- quest_raw[3:nrow(quest_raw),]


baseline_MDA <- quest_dat %>% filter(Visit_ == "Baseline")

baseline_MDA <- Filter(function(x)!all(is.na(x)), baseline_MDA) %>%  # drop columns that are all NA for the given visit
  select(-SubjectID)

quest_unique_responses <- apply(quest_dat[,26:ncol(quest_dat)], 2, unique)
remove(quest_raw)

baseline_MDA_labels <- quest_labels %>% select(colnames(baseline_MDA), -"Visit_")

baseline_MDA_datadict <- rownames_to_column(baseline_MDA_labels %>% t() %>% as.data.frame(), "var_name_MDA") %>% rename(question_text = "V1", qid_raw_text = "V2") %>% 
  mutate(Q.text_clean_MDA = str_replace_all(question_text, pattern = "\n", replacement = " ")) %>% 
  mutate(var_name_prefix = str_split_fixed(var_name_MDA, pattern = "\\d", n = 2)[,1], .after = var_name_MDA) %>% 
  mutate(in_MDA = TRUE)

baseline_MDA_datadict <- baseline_MDA_datadict %>% 
  left_join(y = mda_codebook_quests %>% select(qid_raw_text, qid_clean) %>% mutate(in_codebook = T),
            join_by(qid_raw_text))

baseline_MDA_datadict <- baseline_MDA_datadict %>% relocate(qid_clean, .before = everything()) %>% select(-in_codebook)

remove(baseline_MDA, baseline_MDA_labels, quest_unique_responses)

# -----------------------------------------------------------------------------
# Prep RSR Questionnaire 
#
# -----------------------------------------------------------------------------

baseline_RSR_datadict <- read.xlsx(file.path(path_ontrack_inputs, "OnTrackDataDictionary02032021.xlsx"),
                                sheet = "Baseline")  %>% 
  mutate(Q.text_clean_RSR = str_replace_all(Question.text, pattern = "\n", replacement = " ")) %>%
  mutate(table.column_prefix = str_split_fixed(Table.column, pattern = "\\d", n = 2)[,1], .after = Table.column) %>% 
  mutate(in_RSR = TRUE)

# -----------------------------------------------------------------------------
# Merge MDA and RSR Baseline data dictionaries
# 
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Method 1: Normal Join
# -----------------------------------------------------------------------------
baseline_full_join <- baseline_RSR_datadict %>% 
  full_join(baseline_MDA_datadict,
            by = c("Q.text_clean_RSR" = "Q.text_clean_MDA")) %>% 
  mutate(across(where(is.logical), ~replace_na(.,F)))


if(F){baseline_full_join %>% count(in_RSR, in_MDA)}


baseline_hasconcordence <- baseline_full_join %>% filter(in_RSR & in_MDA)

baseline_inRSR_only <- baseline_full_join %>% filter(in_RSR & !in_MDA)

baseline_inMDA_only <- baseline_full_join %>% filter(!in_RSR & in_MDA)

if(F){ baseline_full_join %>% filter(str_detect(CASES.Item, "DSES") | str_detect(var_name_MDA, "SRH_V1")) %>% View()} # Use for meeting on 9/12/2022

direct_match_crswalk <- baseline_hasconcordence %>% 
  group_by(qid_clean) %>% 
  filter(n() == 1) %>% 
  ungroup() %>% 
  select(question_text, var_name_MDA, var_name_prefix, Table.column, table.column_prefix)



prefix_crswalk <- direct_match_crswalk %>% 
  group_by(var_name_prefix) %>% 
  summarise(table.column_prefix_tomatch = unique(table.column_prefix))
  



crosswalk_beginning <- baseline_full_join %>% #filter(!in_RSR | !in_MDA) %>% 
  full_join(y = prefix_crswalk,
            by = c("var_name_prefix")) %>% 
  mutate(table.column_prefix_tomatch = case_when(
    in_RSR ~ table.column_prefix,
    T ~ table.column_prefix_tomatch),
    table.column_tomatch = Table.column
    ) %>%
  arrange(table.column_prefix_tomatch) %>% 
  group_by(table.column_prefix_tomatch) 

mda_to_crswlk <- crosswalk_beginning %>% filter(in_MDA & !in_RSR) %>% select(qid_clean, var_name_MDA, var_name_prefix, question_text, table.column_prefix_tomatch, table.column_tomatch)



mda_crosswalk_template <- crosswalk_beginning %>% filter(in_MDA) %>% select(qid_clean, var_name_MDA, var_name_prefix, question_text, table.column_prefix_tomatch, table.column_tomatch)

RSR_for_reference_to_build_crosswalk <- baseline_full_join %>% filter(in_RSR) %>% 
  select(Question.text, Table.column, table.column_prefix, in_MDA)


if(F){write.xlsx(mda_to_crswlk, file.path(path_to_questionnaire_crswlk, "mda_to_crswlk_0.xlsx"))}
if(F){write.xlsx(mda_crosswalk_template, file.path(path_to_questionnaire_crswlk, "mda_crosswalk_template.xlsx"))}
if(F){write.xlsx(RSR_for_reference_to_build_crosswalk, file.path(path_to_questionnaire_crswlk, "RSR_backbone_for_reference.xlsx"))}

# The crosswalk will be updated manually using the RSR data dictionary, and the crosswalk template

# -----------------------------------------------------------------------------
# Post-manual updates
#
# ----------------------------------------------------------------------------

mda_crosswalk_post_manual <- read.xlsx(file.path(path_to_questionnaire_crswlk, "mda_quest_crosswalk_manually_updated.xlsx")) %>% 
  select(var_name_MDA, table.column_tomatch, needs_expanded, needs_collapse) 

baseline_MDA_datadict <- baseline_MDA_datadict %>% mutate(var_name_MDA_2 = case_when(
  str_detect(qid_clean, pattern = 'QID645_') ~ str_split_fixed(var_name_MDA, '\\.', n = 2)[,1],  # NIDA1
  str_detect(qid_clean, pattern = 'QID652_') ~ paste0(str_split_fixed(var_name_MDA, '\\.', n = 2)[,1], ".1"),   # NIDA6
  str_detect(qid_clean, pattern = 'QID653_') ~ paste0(str_split_fixed(var_name_MDA, '\\.', n = 2)[,1], ".2"),   # NIDA7
)) %>% 
  mutate(var_name_MDA = coalesce(var_name_MDA_2, var_name_MDA)) %>% 
  select(-var_name_MDA_2)




baseline_MDA_datadict_updated <- baseline_MDA_datadict %>% 
  full_join(y = mda_crosswalk_post_manual,
            by = c("var_name_MDA")) #%>% 
  #filter(!is.na(table.column_tomatch))  # 25 rows of metadata - not questionnaire questions/variables

baseline_MDA_datadict_updated <- baseline_MDA_datadict_updated[!duplicated(baseline_MDA_datadict_updated),]


baseline_MDA_datadict_updated <- baseline_MDA_datadict_updated %>% rename( in_MDA_baseline = in_MDA )



# Join MDA and RSR datadicts

baseline_full_join_updated <- full_join(x = baseline_RSR_datadict %>% filter(!is.na(Table.column)),
          y = baseline_MDA_datadict_updated,
          by = c("Table.column" = "table.column_tomatch"),
          na_matches = "never") %>% 
  mutate(across(where(is.logical), ~replace_na(.,F)))

baseline_full_join_updated <- baseline_full_join_updated %>% rename(in_Utah_baseline = in_RSR, Q.text_clean_Utah = Q.text_clean_RSR)


baseline_full_join_updated %>% count(in_Utah_baseline, in_MDA_baseline)

baseline_not_in_MDA <- baseline_full_join_updated %>% filter(!in_MDA)

full_MDA_quest_datadict_inprogress <- baseline_MDA_datadict_updated 

# ----------------------------------------------------------------------------
# The above process was only looking at the Baseline Questionnaire
# 
# Now need to resolve for v2-v5 questionnaires as well
# ----------------------------------------------------------------------------
# MDA Visit 2

v2_quest_MDA <- quest_dat %>% filter(Visit_ == "Visit 2")

v2_quest_MDA <- Filter(function(x)!all(is.na(x)), v2_quest_MDA)  # drop columns that are all NA for the given visit

v2_quest_MDA_labels <- quest_labels %>% select(colnames(v2_quest_MDA), -"Visit_")

v2_quest_MDA_datadict <- rownames_to_column(v2_quest_MDA_labels %>% t() %>% as.data.frame(), "var_name_MDA") %>% rename(question_text = "V1", qid_raw_text = "V2") %>% 
  mutate(Q.text_clean_MDA = str_replace_all(question_text, pattern = "\n", replacement = " ")) %>% 
  mutate(var_name_prefix = str_split_fixed(var_name_MDA, pattern = "\\d", n = 2)[,1], .after = var_name_MDA) %>% 
  mutate(in_MDA_v2 = TRUE)

v2_quest_MDA_datadict <- v2_quest_MDA_datadict %>% 
  left_join(y = mda_codebook_quests %>% select(qid_raw_text, qid_clean) %>% mutate(in_codebook = T),
            join_by(qid_raw_text))

v2_quest_MDA_datadict <- v2_quest_MDA_datadict %>% relocate(qid_clean, .before = everything()) %>% select(-in_codebook)




full_MDA_quest_datadict_inprogress_2 <- v2_quest_MDA_datadict %>% 
  full_join(full_MDA_quest_datadict_inprogress,
            by = c("qid_clean", "qid_raw_text", "var_name_MDA", "var_name_prefix", "question_text", "Q.text_clean_MDA"))

# MDA Visit 3

v3_quest_MDA <- quest_dat %>% filter(Visit_ == "Visit 3")

v3_quest_MDA <- Filter(function(x)!all(is.na(x)), v3_quest_MDA)  # drop columns that are all NA for the given visit

v3_quest_MDA_labels <- quest_labels %>% select(colnames(v3_quest_MDA), -"Visit_")

v3_quest_MDA_datadict <- rownames_to_column(v3_quest_MDA_labels %>% t() %>% as.data.frame(), "var_name_MDA") %>% rename(question_text = "V1", qid_raw_text = "V2") %>% 
  mutate(Q.text_clean_MDA = str_replace_all(question_text, pattern = "\n", replacement = " ")) %>% 
  mutate(var_name_prefix = str_split_fixed(var_name_MDA, pattern = "\\d", n = 2)[,1], .after = var_name_MDA) %>% 
  mutate(in_MDA_v3 = TRUE)

v3_quest_MDA_datadict <- v3_quest_MDA_datadict %>% 
  left_join(y = mda_codebook_quests %>% select(qid_raw_text, qid_clean) %>% mutate(in_codebook = T),
            join_by(qid_raw_text))

v3_quest_MDA_datadict <- v3_quest_MDA_datadict %>% relocate(qid_clean, .before = everything()) %>% select(-in_codebook)




full_MDA_quest_datadict_inprogress_3 <- v3_quest_MDA_datadict %>% 
  full_join(full_MDA_quest_datadict_inprogress_2,
            by = c("qid_clean", "qid_raw_text", "var_name_MDA", "var_name_prefix", "question_text", "Q.text_clean_MDA"))

if(F){full_MDA_quest_datadict_inprogress_3 %>% count(in_MDA_baseline, in_MDA_v2, in_MDA_v3)}

# MDA Visit 4

v4_quest_MDA <- quest_dat %>% filter(Visit_ == "Visit 4")

v4_quest_MDA <- Filter(function(x)!all(is.na(x)), v4_quest_MDA)  # drop columns that are all NA for the given visit

v4_quest_MDA_labels <- quest_labels %>% select(colnames(v4_quest_MDA), -"Visit_")

v4_quest_MDA_datadict <- rownames_to_column(v4_quest_MDA_labels %>% t() %>% as.data.frame(), "var_name_MDA") %>% rename(question_text = "V1", qid_raw_text = "V2") %>% 
  mutate(Q.text_clean_MDA = str_replace_all(question_text, pattern = "\n", replacement = " ")) %>% 
  mutate(var_name_prefix = str_split_fixed(var_name_MDA, pattern = "\\d", n = 2)[,1], .after = var_name_MDA) %>% 
  mutate(in_MDA_v4 = TRUE)

v4_quest_MDA_datadict <- v4_quest_MDA_datadict %>% 
  left_join(y = mda_codebook_quests %>% select(qid_raw_text, qid_clean) %>% mutate(in_codebook = T),
            join_by(qid_raw_text))

v4_quest_MDA_datadict <- v4_quest_MDA_datadict %>% relocate(qid_clean, .before = everything()) %>% select(-in_codebook)



full_MDA_quest_datadict_inprogress_4 <- v4_quest_MDA_datadict %>% 
  full_join(full_MDA_quest_datadict_inprogress_3,
            by = c("qid_clean", "qid_raw_text", "var_name_MDA", "var_name_prefix", "question_text", "Q.text_clean_MDA")) %>% 
  mutate(across(where(is.logical), ~replace_na(.,F)))

full_MDA_quest_datadict_inprogress_4 %>% count(in_MDA_baseline, in_MDA_v2, in_MDA_v3, in_MDA_v4)

if(F){full_MDA_quest_datadict_inprogress_4 %>% filter(in_MDA_v4 & !in_MDA_baseline & !in_MDA_v2 & !in_MDA_v3) %>% View()}

# MDA Visit 5

v5_quest_MDA <- quest_dat %>% filter(Visit_ == "Visit 5")

v5_quest_MDA <- Filter(function(x)!all(is.na(x)), v5_quest_MDA)  # drop columns that are all NA for the given visit

v5_quest_MDA_labels <- quest_labels %>% select(colnames(v5_quest_MDA), -"Visit_")

v5_quest_MDA_datadict <- rownames_to_column(v5_quest_MDA_labels %>% t() %>% as.data.frame(), "var_name_MDA") %>% rename(question_text = "V1", qid_raw_text = "V2") %>% 
  mutate(Q.text_clean_MDA = str_replace_all(question_text, pattern = "\n", replacement = " ")) %>% 
  mutate(var_name_prefix = str_split_fixed(var_name_MDA, pattern = "\\d", n = 2)[,1], .after = var_name_MDA) %>% 
  mutate(in_MDA_v5 = TRUE)


v5_quest_MDA_datadict <- v5_quest_MDA_datadict %>% 
  left_join(y = mda_codebook_quests %>% select(qid_raw_text, qid_clean) %>% mutate(in_codebook = T),
            join_by(qid_raw_text))

v5_quest_MDA_datadict <- v5_quest_MDA_datadict %>% relocate(qid_clean, .before = everything()) %>% select(-in_codebook)




full_MDA_quest_datadict_inprogress_5 <- v5_quest_MDA_datadict %>% 
  full_join(full_MDA_quest_datadict_inprogress_4,
            by = c("qid_clean", "qid_raw_text", "var_name_MDA", "var_name_prefix", "question_text", "Q.text_clean_MDA")) %>% 
  mutate(across(where(is.logical), ~replace_na(.,F)))




full_MDA_quest_datadict_inprogress_5 %>% count(in_MDA_baseline, in_MDA_v2, in_MDA_v3, in_MDA_v4, in_MDA_v5)

full_MDA_quest_datadict_inprogress_5 %>% filter(in_MDA_v5 & !in_MDA_v4 & !in_MDA_baseline & !in_MDA_v2 & !in_MDA_v3) %>% View()

# -----------------------------------------------------------------------------
# Iteratively update the below in-progress datadict per manual review
# -----------------------------------------------------------------------------
full_MDA_quest_datadict_inprogress_updated <- full_MDA_quest_datadict_inprogress_5 %>% 
  mutate(
    table.column_tomatch = case_when(
      var_name_MDA %in% c("TAC1_V2", "TAC1_V3", "TAC1a_V4", "TAC1c_V4") ~ "TAC1",
      var_name_MDA %in% c("TAC3_V3", "TAC1b_V4") ~ "TAC1a", 
      var_name_MDA %in% c("TAC1d_V4") ~ "TAC1b",
      var_name_MDA %in% c("TAC2_V4") ~ "TAC1c",
      var_name_MDA %in% c("TAC2_V2", "TAC2_V3", "TAC4_V3", "TAC2_V4.1") ~ "TAC2",  # TAC2 used for two different RSR questions
      var_name_MDA %in% c("TAC4_V2", "TAC4_V3.1", "TAC3_V4") ~ "TAC4",
      T ~ table.column_tomatch
    )
  )


tac3_v2 <- full_MDA_quest_datadict_inprogress_updated %>% filter(var_name_MDA == "TAC3_V2") %>% mutate(needs_expanded = TRUE)
tac3_v2_x8 <- tac3_v2 %>% add_row(tac3_v2)%>% add_row(tac3_v2)%>% add_row(tac3_v2)%>% add_row(tac3_v2)%>% add_row(tac3_v2)%>% add_row(tac3_v2)%>% add_row(tac3_v2)
tac3_v2_x8$table.column_tomatch <- paste0("TAC3", letters[1:8])
tac3_v2_row_index <- which(full_MDA_quest_datadict_inprogress_updated$var_name_MDA == "TAC3_V2")
full_MDA_quest_datadict_inprogress_updated <- full_MDA_quest_datadict_inprogress_updated %>% filter(row_number() != tac3_v2_row_index) %>% add_row(tac3_v2_x8)


tac4a_v3 <- full_MDA_quest_datadict_inprogress_updated %>% filter(var_name_MDA == "TAC4a_V3") %>% mutate(needs_expanded = TRUE)
tac4a_v3_x8 <- tac4a_v3 %>% add_row(tac4a_v3)%>% add_row(tac4a_v3)%>% add_row(tac4a_v3)%>% add_row(tac4a_v3)%>% add_row(tac4a_v3)%>% add_row(tac4a_v3)%>% add_row(tac4a_v3)
tac4a_v3_x8$table.column_tomatch <- paste0("TAC3", letters[1:8])
tac4a_v3_row_index <- which(full_MDA_quest_datadict_inprogress_updated$var_name_MDA == "TAC4a_V3")
full_MDA_quest_datadict_inprogress_updated <- full_MDA_quest_datadict_inprogress_updated %>% filter(row_number() != tac4a_v3_row_index) %>% add_row(tac4a_v3_x8)

tac2b_v4 <- full_MDA_quest_datadict_inprogress_updated %>% filter(var_name_MDA == "TAC2b_V4") %>% mutate(needs_expanded = TRUE)
tac2b_v4_x8 <- tac2b_v4 %>% add_row(tac2b_v4)%>% add_row(tac2b_v4)%>% add_row(tac2b_v4)%>% add_row(tac2b_v4)%>% add_row(tac2b_v4)%>% add_row(tac2b_v4)%>% add_row(tac2b_v4)
tac2b_v4_x8$table.column_tomatch <- paste0("TAC3", letters[1:8])
tac2b_v4_row_index <- which(full_MDA_quest_datadict_inprogress_updated$var_name_MDA == "TAC2b_V4")
full_MDA_quest_datadict_inprogress_updated <- full_MDA_quest_datadict_inprogress_updated %>% filter(row_number() != tac2b_v4_row_index) %>% add_row(tac2b_v4_x8)

v4_manual_updated_crswalk <- read.xlsx(file.path(path_to_questionnaire_crswlk, "v4_manual_crosswalk_updated.xlsx")) %>% filter(!is.na(Table.column)) %>% 
  select(var_name_MDA, Table.column, needs_expanded, needs_collapse) %>% mutate(v4_update = TRUE) %>% 
  rename(table.column_tomatch_updated = Table.column,
         needs_expanded_updated = needs_expanded,
         needs_collapse_updated = needs_collapse)

full_MDA_quest_datadict_inprogress_updated <- full_MDA_quest_datadict_inprogress_updated %>% 
  full_join(y = v4_manual_updated_crswalk,
            by = c("var_name_MDA")) %>% 
  mutate(table.column_tomatch = case_when(
    v4_update ~ table.column_tomatch_updated,
    T ~ table.column_tomatch
  ),
  needs_expanded = case_when(
    v4_update ~ needs_expanded_updated,
    T ~ needs_expanded
  ),
  needs_collapse = case_when(
    v4_update ~ needs_collapse_updated,
    T ~ needs_collapse
  )) %>% 
  select(-table.column_tomatch_updated, -needs_collapse_updated, -needs_expanded_updated, -v4_update)

v5_manual_updated_crswlk <- read.xlsx(file.path(path_to_questionnaire_crswlk, "v5_manual_crosswalk_updated.xlsx")) %>% filter(!is.na(Table.column)) %>% select(var_name_MDA, Table.column, needs_expanded, needs_collapse) %>% mutate(v5_update = TRUE) %>% 
  rename(table.column_tomatch_updated = Table.column,
         needs_expanded_updated = needs_expanded,
         needs_collapse_updated = needs_collapse)

full_MDA_quest_datadict_inprogress_updated <- full_MDA_quest_datadict_inprogress_updated %>% 
  full_join(y = v5_manual_updated_crswlk,
            by = c("var_name_MDA")) %>% 
  mutate(table.column_tomatch = case_when(
    v5_update ~ table.column_tomatch_updated,
    T ~ table.column_tomatch
  ),
  needs_expanded = case_when(
    v5_update ~ needs_expanded_updated,
    T ~ needs_expanded
  ),
  needs_collapse = case_when(
    v5_update ~ needs_collapse_updated,
    T ~ needs_collapse
  )) %>% 
  select(-table.column_tomatch_updated, -needs_collapse_updated, -needs_expanded_updated, -v5_update)



# -----------------------------------------------------------------------------
# Visit 2
# 
# -----------------------------------------------------------------------------
# get RSR datadict for v2
v2_Utah_datadict <- read.xlsx(file.path(path_ontrack_inputs, "OnTrackDataDictionary02032021.xlsx"),
                                   sheet = "Visit 2")  %>% 
  mutate(Q.text_clean_Utah = str_replace_all(Question.text, pattern = "\n", replacement = " ")) %>%
  mutate(table.column_prefix = str_split_fixed(Table.column, pattern = "\\d", n = 2)[,1], .after = Table.column) %>% 
  mutate(in_v2_Utah = TRUE)

v2_full_join <- v2_Utah_datadict %>% 
  full_join(full_MDA_quest_datadict_inprogress_updated %>% filter(in_MDA_v2),
            by = c("Table.column" = "table.column_tomatch"),
            na_matches = "never") %>% 
  mutate(across(where(is.logical), ~replace_na(.,F))) #%>% 
  #filter(!(in_v2_Utah & is.na(Table.column)))

v2_full_join %>% count(in_v2_Utah, in_MDA_v2)

if(F){v2_full_join %>% filter(!in_v2_Utah | !in_MDA_v2) %>% View}


# -----------------------------------------------------------------------------
# Visit 3
# 
# -----------------------------------------------------------------------------
# get RSR datadict for v3
v3_Utah_datadict <- read.xlsx(file.path(path_ontrack_inputs, "OnTrackDataDictionary02032021.xlsx"),
                             sheet = "Visit 3")  %>% 
  mutate(Q.text_clean_Utah = str_replace_all(Question.text, pattern = "\n", replacement = " ")) %>%
  mutate(table.column_prefix = str_split_fixed(Table.column, pattern = "\\d", n = 2)[,1], .after = Table.column) %>% 
  mutate(in_v3_Utah = TRUE)

v3_full_join <- v3_Utah_datadict %>% 
  full_join(full_MDA_quest_datadict_inprogress_updated %>% filter(in_MDA_v3),
            by = c("Table.column" = "table.column_tomatch"),
            na_matches = "never") %>% 
  mutate(across(where(is.logical), ~replace_na(.,F))) #%>% 
  #filter(!(in_v3_Utah & is.na(Table.column)))

v3_full_join %>% count(in_v3_Utah, in_MDA_v3)

if(F){v3_full_join %>% filter(!in_v3_Utah | !in_MDA_v3) %>% View}


# -----------------------------------------------------------------------------
# Visit 4
# 
# Visit 4 asked questions not asked in baseline nor v2 & v3
#
# -----------------------------------------------------------------------------
# get Utah datadict for v4
v4_Utah_datadict <- read.xlsx(file.path(path_ontrack_inputs, "OnTrackDataDictionary02032021.xlsx"),
                             sheet = "Visit 4")  %>% 
  mutate(Q.text_clean_Utah = str_replace_all(Question.text, pattern = "\n", replacement = " ")) %>%
  mutate(table.column_prefix = str_split_fixed(Table.column, pattern = "\\d", n = 2)[,1], .after = Table.column) %>% 
  mutate(in_v4_Utah = TRUE)

v4_full_join <- v4_Utah_datadict %>% 
  full_join(full_MDA_quest_datadict_inprogress_updated %>% filter(in_MDA_v4),
            by = c("Table.column" = "table.column_tomatch"),
            na_matches = "never") %>% 
  mutate(across(where(is.logical), ~replace_na(.,F))) #%>% 
  #filter(!(in_v4_Utah & is.na(Table.column)))

v4_full_join %>% count(in_v4_Utah, in_MDA_v4)

if(F){v4_full_join %>% filter(!in_v4_Utah | !in_MDA_v4) %>% View}

# Several new questions that did not occur in the previous questionnaires

v4_non_matched <- v4_full_join %>% filter(!in_v4_Utah | !in_MDA_v4)

if(F){write.xlsx(v4_non_matched,
           file.path(path_to_questionnaire_crswlk, "v4_manual_crosswalk_template.xlsx"))}


# -----------------------------------------------------------------------------
# Visit 5
#
# -----------------------------------------------------------------------------
# get RSR datadict for v5
v5_Utah_datadict <- read.xlsx(file.path(path_ontrack_inputs, "OnTrackDataDictionary02032021.xlsx"),
                             sheet = "Visit 5")  %>% 
  mutate(Q.text_clean_Utah = str_replace_all(Question.text, pattern = "\n", replacement = " ")) %>%
  mutate(table.column_prefix = str_split_fixed(Table.column, pattern = "\\d", n = 2)[,1], .after = Table.column) %>% 
  mutate(in_v5_Utah = TRUE)

v5_full_join <- v5_Utah_datadict %>% 
  full_join(full_MDA_quest_datadict_inprogress_updated %>% filter(in_MDA_v5),
            by = c("Table.column" = "table.column_tomatch"),
            na_matches = "never") %>% 
  mutate(across(where(is.logical), ~replace_na(.,F))) #%>% 
  #filter(!(in_v5_Utah & is.na(Table.column)))

v5_full_join %>% count(in_v5_Utah, in_MDA_v5)

if(F){v5_full_join %>% filter(!in_v5_Utah | !in_MDA_v5) %>% View}

v5_non_matched <- v5_full_join %>% filter(!in_v5_Utah | !in_MDA_v5)

if(F){write.xlsx(v5_non_matched,
                 file.path(path_to_questionnaire_crswlk, "v5_manual_crosswalk_template.xlsx"))}

# ------------------------------------------------------------------------------
# Summarize
#
# ------------------------------------------------------------------------------

baseline_full_join_updated <- baseline_full_join_updated %>% clean_names()
baseline_full_join_updated %>% count(in_Utah_baseline, in_MDA_baseline)
baseline_non_matches <- baseline_full_join_updated %>% filter(!in_Utah_baseline | !in_MDA_baseline)

v2_full_join <- v2_full_join %>% clean_names()
v2_full_join %>% count(in_v2_Utah, in_MDA_v2)
v2_non_matches <- v2_full_join %>% filter(!in_v2_Utah | !in_MDA_v2)

v3_full_join <- v3_full_join %>% clean_names()
v3_full_join %>% count(in_v3_Utah, in_MDA_v3)
v3_non_matches <- v3_full_join %>% filter(!in_v3_Utah | !in_MDA_v3)

v4_full_join <- v4_full_join %>% clean_names()
v4_full_join %>% count(in_v4_Utah, in_MDA_v4)
v4_non_matches <- v4_full_join %>% filter(!in_v4_Utah | !in_MDA_v4)

v5_full_join <- v5_full_join %>% clean_names()
v5_full_join %>% count(in_v5_Utah, in_MDA_v5)
v5_non_matches <- v5_full_join %>% filter(!in_v5_Utah | !in_MDA_v5)


if(T){
  # write to summary excel file
  wb <- createWorkbook()
  addWorksheet(wb, "Summary")
  addWorksheet(wb, "Baseline quest crosswalk")
  addWorksheet(wb, "v2 quest crosswalk")
  addWorksheet(wb, "v3 quest crosswalk")
  addWorksheet(wb, "v4 quest crosswalk")
  addWorksheet(wb, "v5 quest crosswalk")
  
  writeData(wb, sheet = 1, baseline_full_join_updated %>% count(in_utah_baseline, in_mda_baseline), startCol = 1, startRow = 2)
  writeData(wb, sheet = 1, v2_full_join %>% count(in_v2_utah, in_mda_v2), startCol = 5, startRow = 2)
  writeData(wb, sheet = 1, v3_full_join %>% count(in_v3_utah, in_mda_v3), startCol = 9, startRow = 2)
  writeData(wb, sheet = 1, v4_full_join %>% count(in_v4_utah, in_mda_v4), startCol = 13, startRow = 2)
  writeData(wb, sheet = 1, v5_full_join %>% count(in_v5_utah, in_mda_v5), startCol = 17, startRow = 2)
  
  writeData(wb, sheet = 2, baseline_full_join_updated, rowNames = FALSE)
  writeData(wb, sheet = 3, v2_full_join, rowNames = FALSE)
  writeData(wb, sheet = 4, v3_full_join, rowNames = FALSE)
  writeData(wb, sheet = 5, v4_full_join, rowNames = FALSE)
  writeData(wb, sheet = 6, v5_full_join, rowNames = FALSE)
  
  # writeData(wb, sheet = 2, baseline_non_matches, rowNames = FALSE)
  # writeData(wb, sheet = 3, v2_non_matches, rowNames = FALSE)
  # writeData(wb, sheet = 4, v3_non_matches, rowNames = FALSE)
  # writeData(wb, sheet = 5, v4_non_matches, rowNames = FALSE)
  # writeData(wb, sheet = 6, v5_non_matches, rowNames = FALSE)
  
  saveWorkbook(wb, file.path(path_to_questionnaire_crswlk, "questionnaire_concordance_final.xlsx"), overwrite = TRUE) # refer to format from "questionnaire_concordance_formatted.xlsx"
  saveWorkbook(wb, file.path(path_ontrack_questionnaire_inputs, "questionnaire_concordance_final.xlsx"), overwrite = TRUE)
}

# ---------------------------------------------------------------
# Creating master crosswalk
#
# ---------------------------------------------------------------

baseline_utah_datadict <- baseline_RSR_datadict %>% rename(in_baseline_Utah = in_RSR, Q.text_clean_Utah = Q.text_clean_RSR)

by <- v2_Utah_datadict %>% select(-in_v2_Utah) %>% colnames()

utah_datadict_full_join <- baseline_utah_datadict %>% 
  full_join(y = v2_Utah_datadict %>% mutate(Output.Variable.Name = as.character(Output.Variable.Name)),
            by) %>% 
  full_join(y = v3_Utah_datadict %>% mutate(Output.Variable.Name = as.character(Output.Variable.Name)),
            by) %>% 
  full_join(y = v4_Utah_datadict %>% mutate(Output.Variable.Name = as.character(Output.Variable.Name)),
            by)  %>% 
  full_join(y = v5_Utah_datadict %>% mutate(Output.Variable.Name = as.character(Output.Variable.Name)),
            by) %>% 
  mutate(across(where(is.logical), ~replace_na(.,F)))


if(F){utah_datadict_full_join %>% count(in_baseline_Utah, in_v2_Utah, in_v3_Utah, in_v4_Utah, in_v5_Utah) %>% View}


crosswalk_main <-  utah_datadict_full_join %>% 
  full_join(y = full_MDA_quest_datadict_inprogress_updated,
            by = c("Table.column" = "table.column_tomatch"),
            na_matches = "never") %>% 
  mutate(across(where(is.logical), ~replace_na(.,F)))

crosswalk_main <- crosswalk_main %>% select(-c(table.column_prefix, Database.table, var_name_prefix))
crosswalk_main <- crosswalk_main %>% relocate(Comments, .after = Q.text_clean_Utah) %>% 
  relocate(in_MDA_baseline, in_MDA_v2, in_MDA_v3, in_MDA_v4, in_MDA_v5, .after = Q.text_clean_MDA)

#crosswalk_main %>% count(in_any_Utah = any(in_baseline_Utah, in_v2_Utah, in_v3_Utah, in_v4_Utah, in_v5_Utah), not_in_any_MDA = !any(in_MDA_baseline, in_MDA_v2, in_MDA_v3, in_MDA_v4, in_MDA_v5))
#crosswalk_main %>% count(in_any_Utah = any(in_baseline_Utah, in_v2_Utah, in_v3_Utah, in_v4_Utah, in_v5_Utah), in_any_MDA = any(in_MDA_baseline, in_MDA_v2, in_MDA_v3, in_MDA_v4, in_MDA_v5))

crosswalk_main <- crosswalk_main %>% clean_names()

crosswalk_main %>% count(in_baseline_utah, in_mda_baseline)                        
crosswalk_main %>% count(in_v2_utah, in_mda_v2)

write_csv(crosswalk_main,
          file = file.path(path_ontrack_questionnaire_inputs, "mda_to_Utah_questionnaire_crosswalk.csv"))
saveRDS(crosswalk_main,
        file = file.path(path_ontrack_questionnaire_inputs, "mda_to_Utah_questionnaire_crosswalk.RDS"))
write_dta(crosswalk_main,
          file.path(path_ontrack_questionnaire_inputs, "mda_to_Utah_questionnaire_crosswalk.dta"))



# ---------------------------------------------------------------
# Data quality check on the RSR data dictionary
# ---------------------------------------------------------------

baseline_RSR_datadict <- read.xlsx(file.path(path_ontrack_inputs, "OnTrackDataDictionary02032021.xlsx"),
                                   sheet = "Baseline")  %>% 
  mutate(Q.text_clean_RSR = str_replace_all(Question.text, pattern = "\n", replacement = " ")) %>%
  mutate(table.column_prefix = str_split_fixed(Table.column, pattern = "\\d", n = 2)[,1], .after = Table.column) %>% 
  mutate(in_RSR = TRUE)

v2_RSR_datadict <- read.xlsx(file.path(path_ontrack_inputs, "OnTrackDataDictionary02032021.xlsx"),
                             sheet = "Visit 2")  %>% 
  mutate(Q.text_clean_RSR = str_replace_all(Question.text, pattern = "\n", replacement = " ")) %>%
  mutate(table.column_prefix = str_split_fixed(Table.column, pattern = "\\d", n = 2)[,1], .after = Table.column) %>% 
  mutate(in_v2_RSR = TRUE)

v3_RSR_datadict <- read.xlsx(file.path(path_ontrack_inputs, "OnTrackDataDictionary02032021.xlsx"),
                             sheet = "Visit 3")  %>% 
  mutate(Q.text_clean_RSR = str_replace_all(Question.text, pattern = "\n", replacement = " ")) %>%
  mutate(table.column_prefix = str_split_fixed(Table.column, pattern = "\\d", n = 2)[,1], .after = Table.column) %>% 
  mutate(in_v3_RSR = TRUE)

v4_RSR_datadict <- read.xlsx(file.path(path_ontrack_inputs, "OnTrackDataDictionary02032021.xlsx"),
                             sheet = "Visit 4")  %>% 
  mutate(Q.text_clean_RSR = str_replace_all(Question.text, pattern = "\n", replacement = " ")) %>%
  mutate(table.column_prefix = str_split_fixed(Table.column, pattern = "\\d", n = 2)[,1], .after = Table.column) %>% 
  mutate(in_v4_RSR = TRUE)

v5_RSR_datadict <- read.xlsx(file.path(path_ontrack_inputs, "OnTrackDataDictionary02032021.xlsx"),
                             sheet = "Visit 5")  %>% 
  mutate(Q.text_clean_RSR = str_replace_all(Question.text, pattern = "\n", replacement = " ")) %>%
  mutate(table.column_prefix = str_split_fixed(Table.column, pattern = "\\d", n = 2)[,1], .after = Table.column) %>% 
  mutate(in_v5_RSR = TRUE)


quest_RSR_all <- read.xlsx(file.path(path_ontrack_inputs, "OnTrack Questionnaire Data.xlsx"))

quest_RSR_baseline <- quest_RSR_all %>% filter(QuestType == "Baseline")
quest_RSR_baseline <- Filter(function(x)!all(is.na(x)), quest_RSR_baseline)  # drop columns that are all NA for the given visit
quest_RSR_baseline_vars <- data.frame(variable = colnames(quest_RSR_baseline %>% select(-c(SubjectID, Enrolled_ID, QuestionnaireID, QuestType, QuestDate))),
                                      in_quest_dat = TRUE)

quest_RSR_visit2 <- quest_RSR_all %>% filter(QuestType == "Visit2")
quest_RSR_visit2 <- Filter(function(x)!all(is.na(x)), quest_RSR_visit2)  # drop columns that are all NA for the given visit
quest_RSR_visit2_vars <- data.frame(variable = colnames(quest_RSR_visit2 %>% select(-c(SubjectID, Enrolled_ID, QuestionnaireID, QuestType, QuestDate))),
                                      in_quest_dat = TRUE)

quest_RSR_visit3 <- quest_RSR_all %>% filter(QuestType == "Visit3")
quest_RSR_visit3 <- Filter(function(x)!all(is.na(x)), quest_RSR_visit3)  # drop columns that are all NA for the given visit
quest_RSR_visit3_vars <- data.frame(variable = colnames(quest_RSR_visit3 %>% select(-c(SubjectID, Enrolled_ID, QuestionnaireID, QuestType, QuestDate))),
                                    in_quest_dat = TRUE)

quest_RSR_visit4 <- quest_RSR_all %>% filter(QuestType == "Visit4")
quest_RSR_visit4 <- Filter(function(x)!all(is.na(x)), quest_RSR_visit4)  # drop columns that are all NA for the given visit
quest_RSR_visit4_vars <- data.frame(variable = colnames(quest_RSR_visit4 %>% select(-c(SubjectID, Enrolled_ID, QuestionnaireID, QuestType, QuestDate))),
                                    in_quest_dat = TRUE)

quest_RSR_visit5 <- quest_RSR_all %>% filter(QuestType == "Visit5")
quest_RSR_visit5 <- Filter(function(x)!all(is.na(x)), quest_RSR_visit5)  # drop columns that are all NA for the given visit
quest_RSR_visit5_vars <- data.frame(variable = colnames(quest_RSR_visit5 %>% select(-c(SubjectID, Enrolled_ID, QuestionnaireID, QuestType, QuestDate))),
                                    in_quest_dat = TRUE)

# Baseline
full_join_RSR_baseline_vars <- baseline_RSR_datadict %>% filter(!is.na(Table.column)) %>% 
  full_join(y = quest_RSR_baseline_vars,
            by = c("Table.column" = "variable")) %>% 
  mutate(across(where(is.logical), ~replace_na(.,F)))


full_join_RSR_baseline_vars %>% count(in_RSR, in_quest_dat)

full_join_RSR_baseline_vars %>% filter(!in_RSR | !in_quest_dat) %>% View


# Visit 2
full_join_RSR_visit2_vars <- v2_RSR_datadict %>% filter(!is.na(Table.column)) %>% 
  full_join(y = quest_RSR_visit2_vars,
            by = c("Table.column" = "variable")) %>% 
  mutate(across(where(is.logical), ~replace_na(.,F)))


full_join_RSR_visit2_vars %>% count(in_v2_RSR, in_quest_dat)

full_join_RSR_visit2_vars %>% filter(!in_v2_RSR | !in_quest_dat) %>% View


# Visit 3
full_join_RSR_visit3_vars <- v3_RSR_datadict %>% filter(!is.na(Table.column)) %>% 
  full_join(y = quest_RSR_visit3_vars,
            by = c("Table.column" = "variable")) %>% 
  mutate(across(where(is.logical), ~replace_na(.,F)))


full_join_RSR_visit3_vars %>% count(in_v3_RSR, in_quest_dat)

full_join_RSR_visit3_vars %>% filter(!in_v3_RSR | !in_quest_dat) %>% View


# Visit 4
full_join_RSR_visit4_vars <- v4_RSR_datadict %>% filter(!is.na(Table.column)) %>% 
  full_join(y = quest_RSR_visit4_vars,
            by = c("Table.column" = "variable")) %>% 
  mutate(across(where(is.logical), ~replace_na(.,F)))


full_join_RSR_visit4_vars %>% count(in_v4_RSR, in_quest_dat)

full_join_RSR_visit4_vars %>% filter(!in_v4_RSR | !in_quest_dat) %>% View


# Visit 5
full_join_RSR_visit5_vars <- v5_RSR_datadict %>% filter(!is.na(Table.column)) %>% 
  full_join(y = quest_RSR_visit5_vars,
            by = c("Table.column" = "variable")) %>% 
  mutate(across(where(is.logical), ~replace_na(.,F)))


full_join_RSR_visit5_vars %>% count(in_v5_RSR, in_quest_dat)

full_join_RSR_visit5_vars %>% filter(!in_v5_RSR | !in_quest_dat) %>% View







