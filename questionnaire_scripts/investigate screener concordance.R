library(openxlsx)
library(tidyr)
library(tibble)
library(fuzzyjoin)
library(stringr)

source("paths.R")


screener_RSR_datadict <- read.xlsx(file.path(path_ontrack_inputs, "OnTrackDataDictionary02032021.xlsx"),
                                   sheet = "Screener") %>% 
  mutate(Q.text_clean_RSR = str_replace_all(Question.text, pattern = "\n", replacement = " ")) %>% 
  mutate(in_RSR = TRUE)

screener_MDA_datadict <- read.xlsx(file.path(path_ontrack_mda_raw, "TblScreenPhone Questions.xlsx")) %>% 
  mutate(Q.text_clean_MDA = str_replace_all(Question, pattern = "\n", replacement = " ")) %>%
  mutate(Q.text_clean_MDA = str_remove(Q.text_clean_MDA, "\\d+\\) ")) %>% 
  mutate(in_MDA = TRUE)


# ------------------------------------------------------------------------------

screener_full_join = screener_RSR_datadict %>% 
  full_join(screener_MDA_datadict,
            by = c("Q.text_clean_RSR"="Q.text_clean_MDA"))

screener_full_join %>% count(in_RSR, in_MDA)





































