


## Mikael Poul Johannesson
## 2021

## Trenger først: dommere og parter, hente fra case_string
## saksnr, dato, dommere, parter,  siteringer, topic labels.
## case string; den har text og html-koder

## Start matter ------------------------------------------------------

library(here)
library(readxl)
library(rjson)
library(tidyverse)
library(rvest)
library(xml2)
library(lubridate)
library(furrr)
library(fuzzyjoin)
library(stringdist)

cores <- parallel::detectCores()
plan(multicore, workers = cores)

## List of SC judges -------------------------------------------------

judges_list_raw <- read_excel(
  here::here("raw", "Justices_Denmark.xlsx")
)

judges_list <-
  judges_list_raw %>%
  rename(judge_name = jfname, judge_name_fam = jfamname) %>%
  group_by(judge_name, judge_name_fam) %>%
  summarize(
    judge_year_started = min(ystarted),
    judge_year_ended = ifelse(is.na(max(yended)), Inf, max(yended)),
    been_chief = as.numeric(sum(chief) > 0)
  ) %>%
  ungroup()

## Meta data ---------------------------------------------------------

## dk_get_meta_data <- function(.file) {44

##   raw <-
##     here::here("raw", "goteborg-dk-20201201-12k-docs", .file) %>%
##     readLines() %>%
##     fromJSON()

##   tibble(
##     case_filename = .file,
##     case_id = raw$metadata$document_id,
##     case_title = raw$metadata$title,
##     case_date = as.Date(raw$metadata$issue_date),
##     case_topic_labels = list(raw$metadata$topic_labels),
##     case_topic_codes = list(raw$metadata$topic_codes),
##     case_citations = list(raw$metadata$citations),
##     case_document_category = raw$metadata$document_category
##   )
## }

## dk_files <-
##   here::here("raw", "goteborg-dk-20201201-12k-docs") %>%
##   list.files()

## case_meta <-
##   dk_files %>%
##   future_map_dfr(dk_get_meta_data)

## saveRDS(dk_case_meta, here::here("data", "dk_metadata.rds"))

## Function for extracting case parts from raw xml -------------------

get_parts <- function(raw_xml, .file) {

  parts <- NA

  if (!is.na(raw_xml %>% xml_node("PART"))) {

    parts_raw <-
      raw_xml %>%
      xml_nodes("PART") %>%
      xml_text()

    has_lwr <- grepl("adv\\.|lrs\\.|hrs\\.|(selv)|ved advokat|\\(advokat |kammeradvokaten", parts_raw)

    ## Assumption:
    ## - lrs = "landsretssagfører" = lawyer;
    ## - hrs = "højesteretssagfører" = lawyer

    parts <- tibble(
      part_raw = parts_raw,
      part_name = case_when(
        has_lwr & grepl("\\(selv\\)", parts_raw) ~ gsub("^(.*) \\(selv\\).*$", "\\1", parts_raw),
        has_lwr ~ gsub("^(.*) \\(.*(adv|lrs|hrs|kammeradvokaten|advokat ).*$", "\\1", parts_raw),
        !has_lwr ~ parts_raw),
      part_n = seq_len(length(parts_raw)),
      part_lawyer = case_when(
        has_lwr & grepl("\\(selv\\)", parts_raw) ~ as.character(NA),
        has_lwr & grepl("\\(km.adv", parts_raw) ~ as.character(NA),
        has_lwr & !grepl("\\(km.adv", parts_raw) ~ gsub("^.*\\(.*(adv\\.|lrs\\.|hrs\\.|advokat )(.*).*$", "\\2", parts_raw),
        !has_lwr ~ as.character(NA)),
      part_lawyer_postnote = case_when(
        grepl(",", part_lawyer) ~ gsub("(.*?)(,+.*)", "\\2", part_lawyer),
        TRUE ~ as.character(NA)),
      part_lawyer_type = case_when(
        !has_lwr ~ as.character(NA),
        grepl("kammeradvokaten", parts_raw) ~ "km.adv",
        grepl("km.adv", parts_raw) ~ "km.adv",
        grepl("advokat ", parts_raw) ~ "adv",
        grepl("adv\\.", parts_raw) ~ "adv",
        grepl("lrs\\.", parts_raw) ~ "lrs",
        grepl("hrs\\.", parts_raw) ~ "hrs",
        grepl("\\(selv\\)", parts_raw) ~ "self",
        grepl("\\(selvmøder\\)", parts_raw) ~ "self (selvmøder)"),
      part_lawyer_self = case_when(
        has_lwr & grepl("(selv)", parts_raw) ~ 1,
        has_lwr & !grepl("(selv)", parts_raw) ~ 0,
        !has_lwr ~ 0),
      part_lawyer_kmadv = case_when(
        has_lwr & grepl("km.adv.", parts_raw) ~ 1,
        has_lwr  ~ 0,
        !has_lwr ~ 0),
      case_filename = .file
    ) %>%
      mutate(
        part_lawyer = part_lawyer %>%
          gsub("(.*?),+.*", "\\1", .) %>%
          gsub("(^.*)\\).*$", "\\1", .) %>%
          str_trim() %>%
          gsub("\\.$", "", .) %>%
          gsub("\\)$", "", .)
      ) %>%
      mutate_at(vars(part_name, part_lawyer), str_trim)

  }
  parts

}

## Function for extracting judges from raw xml -----------------------

## Function for extracting the court judge in dk data.
## Only sort out the first set of words (defined by a whitespace
## token) that starts with an upper case letter, to extract judge
## name from other content following their mention.
clean_judge_tokens <- function(x) {

  x <- x[x != ""]

  if (any(!grepl("^[A-ZÆØÅ].*$", x))) {
    x <-  x[1:(which(!grepl("^[A-ZÆØÅ].*$", x))[1] - 1)]
  }

  x %>%
    gsub("\\W", " ", .) %>%
    gsub(" +", " ", .) %>%
    str_trim() %>%
    paste0(collapse = " ")
}

get_judges <- function(raw_xml, .file, .case_date) {

  judge_raw <- NA
  judges <- NA

  has_dommere_node <- !is.na(raw_xml %>% xml_node("DOMMERE"))
  has_dommer_node <- !is.na(raw_xml %>% xml_node("DOMMER"))
  has_tb_node <- !is.na(raw_xml %>% xml_node("TB"))
  has_dommere_text  <-
    raw_xml %>%
    xml_nodes("TXT") %>%
    xml_text() %>%
    paste0(collapse = " ") %>%
    grepl(" dommere:", .)

  if (has_dommer_node) {

    judge_raw <-
      raw_xml %>%
      xml_nodes("DOMMER") %>%
      xml_text()

  } else if (has_tb_node) {

    judge_raw <-
      raw_xml %>%
      xml_nodes("TB") %>%
      xml_text()

    ## ## Make sure the TB node is actually a judge.
    if (!any(grepl("^[A-ZÆØÅ].*$", unlist(strsplit(judge_raw, " "))))) {
      judge_raw <- NA
      has_tb_node <- FALSE
    }

  } else if (has_dommere_text) {

    judge_raw <-
      raw_xml %>%
      xml_nodes("TXT") %>%
      xml_text() %>%
      grep(" dommere:", ., value = TRUE) %>%
      gsub("^.* dommere:(.*)$", "\\1", .) %>%
      strsplit(",|og") %>%
      unlist() %>%
      str_trim()

  }

  if (!is.na(judge_raw) || is.character(judge_raw) || length(judge_raw) > 1) {

    ## Only keep judge names that match any name from the list of sc
    ## judges; remove too long names, since they typically are based
    ## on mentions where a judge states something.
    ## judge_raw <- judge_raw[check_possible_names(judge_raw)]
    ## judge_raw <- judge_raw[nchar(judge_raw) < 40]

    ## Til filter:
    ## - rule for funch jensen;
    ## - remove: "udtaler" [og tekst som kommer etter], "og" [og tekst etter],  "kst" [og tekst etter]
    ## -> then match complete name -> but deal with letter-only first name (e.g., P. Christiensen)
    ## + check if other matches are nearby
    ##   - get the whole text and find placement?
    ##   -

    potential_judges_fam <-
      judges_list %>%
      mutate(
        ystart = ifelse(is.na(judge_year_started), 1900, judge_year_started),
        yend = ifelse(judge_year_ended == Inf, 2021, judge_year_ended),
        years = map2(ystart, yend, seq, 1),
        judge_fam_tks =
          judge_name_fam %>%
          strsplit(" ") %>%
          map_chr(clean_judge_tokens) %>%
          tolower() %>%
          strsplit(" ")
      ) %>%
      filter(map_lgl(years, ~ any(. %in% year(.case_date)))) %>%
      unnest(judge_fam_tks) %>%
      distinct(judge_name, judge_fam_tks) %>%
      rename(judge_name_fam = judge_name)

    potential_judges <-
      judges_list %>%
      mutate(
        ystart = ifelse(is.na(judge_year_started), 1900, judge_year_started),
        yend = ifelse(judge_year_ended == Inf, 2021, judge_year_ended),
        years = map2(ystart, yend, seq, 1),
        judge_name_clean =
          judge_name %>%
          strsplit(" ") %>%
          map_chr(clean_judge_tokens) %>%
          tolower(),
        judge_tks =
          judge_name %>%
          strsplit(" ") %>%
          map_chr(clean_judge_tokens) %>%
          tolower() %>%
          strsplit(" ")
      ) %>%
      filter(map_lgl(years, ~ any(. %in% year(.case_date)))) %>%
      unnest(judge_tks) %>%
      distinct(judge_name, judge_tks, judge_name_clean)



    ## Jensen --> always at least two names, amtch only if two matches
    ## Other problems could be solved by first matching on lst name,
    ## then all; if jensen -- demand at least two names

    judges_raw <- tibble(
      judge_name_raw = judge_raw[nchar(judge_raw) < 40],
      judge_clean = judge_name_raw %>% map_chr(clean_judge_tokens) %>% tolower(),
      judge_clean_tks = judge_clean %>% strsplit(" |,"),
      judge_order_mentioned =
        judge_name_raw %>%
        length() %>%
        seq_len(),
      judge_n_name_tokens =
        judge_name_raw %>%
        strsplit(" |,") %>%
        map_dbl(length),
      judge_konst_any_detected =
        judge_name_raw %>%
        str_detect("kst\\.") %>%
        any() %>%
        as.numeric(),
      judge_konst =
        judge_name_raw %>%
        str_detect("kst\\.") %>%
        as.numeric(),
      judge_xml_source = case_when(
        has_dommer_node ~ "dommer_node",
        has_tb_node ~ "tb_node",
        has_dommere_text ~ "dommere_text"),
      case_filename = .file
    )

    ## 1. Judges are always mentioned by (minimum) the family name, so
    ## first matching is done by fam name to get a reduce set of
    ## candidate judges;
    ## 2. then all name tokens (as in all tokens of
    ## their full name) of these candidate judges are matched against
    ## all tokens from the list
    ## 3. and the one/ones with the lowest
    ## stringdistance (by yptimal string aligment / restricted
    ## Damerau-Levenshtein distance).

    ## POTENTIOAL PROBLEM:
    ## - MULTIPLE MATCHES IN LAST STEP
    ## -> "Funch Jensen" becomes torben jensen: give rule to always match funch with henrik?
    ## - matches when multiple (lagmanns-)judges are listed; set a max stringdist?
    ## Jensen matches - > force more than one name to be used for id jensen specifically
    ## olav d larsen -> lars bay larsen.
    judges_name_match <-
      judges_raw %>%
      unnest(judge_clean_tks) %>%
      left_join(
        potential_judges_fam,
        by = c("judge_clean_tks" = "judge_fam_tks")
      ) %>%
      filter(!is.na(judge_name_fam)) %>%
      left_join(
        potential_judges,
        by = c("judge_clean_tks" = "judge_tks")
      ) %>%
      filter(!is.na(judge_name)) %>%
      distinct(judge_clean, judge_name, judge_name_clean) %>%
      mutate(judge_stringdist = stringdist(judge_clean, judge_name_clean)) %>%
      group_by(judge_clean) %>%
      filter(judge_stringdist == min(judge_stringdist, na.rm = TRUE)) %>%
      mutate(judge_right_matches = n()) %>%
      ungroup() %>%
      group_by(judge_name) %>%
      mutate(judge_left_matches = n()) %>%
      ungroup()

    judges <-
      judges_raw %>%
      inner_join(judges_name_match, by = "judge_clean") %>%
      group_by(case_filename, judge_name, judge_xml_source) %>%
      summarize(
        judge_clean = list(unique(judge_clean)),
        judge_name_raw = list(unique(judge_name_raw)),
        judge_order_mentioned = min(judge_order_mentioned, na.rm = TRUE)
      ) %>%
      ungroup() %>%
      arrange(judge_order_mentioned)

  }

  judges

}

## Input: .file = filename
fetch_dk_data <- function(.file) {

  raw <-
    here::here("raw", "goteborg-dk-20201201-12k-docs", .file) %>%
    readLines(warn = FALSE) %>%
    fromJSON()

  raw_xml <- read_xml(raw$case_string)

  ## The new dplyr summarize() warning BS is irritating as f.
  sumform_opt_default <- options("dplyr.summarise.inform")
  options(dplyr.summarise.inform = FALSE)

  out <- tibble(
    case_filename = .file,
    case_id = raw$metadata$document_id,
    case_title = raw$metadata$title,
    case_date = as.Date(raw$metadata$issue_date),
    case_text_raw = raw_xml %>% xml_text(),
    case_topic_labels = list(raw$metadata$topic_labels),
    case_topic_codes = list(raw$metadata$topic_codes),
    case_citations = list(raw$metadata$citations),
    case_document_category = raw$metadata$document_category,
    case_judges = raw_xml %>% get_judges(.file, case_date) %>% list(),
    case_parts = raw_xml %>% get_parts(.file) %>% list()
  )

  options(dplyr.summarise.inform = sumform_opt_default)

  out
}

## Read all files and save raw data ----------------------------------

## ## Time estimate:
## t1 <- proc.time()
## case_data <-
##   files %>%
##   sample(100) %>%
##   future_map_dfr(fetch_dk_data)
## t2 <- proc.time()
## (t2-t1) * (11031 / 100) # secs
## ((t2-t1) * (11031 / 100)) / 60 # min

dk_files <-
  here::here("raw", "goteborg-dk-20201201-12k-docs") %>%
  list.files()

dk_case_data_raw <- future_map_dfr(dk_files, fetch_dk_data)

saveRDS(dk_case_data_raw, here::here("data", "dk_case_data_raw.rds"))

## Split into separate datasets --------------------------------------

dk_case_data_raw <- readRDS(here::here("data", "dk_case_data_raw.rds"))

## Trenger først: dommere og parter, hente fra case_string
## saksnr, dato, dommere, parter,  siteringer, topic labels.
## case string; den har text og html-koder

## Case data.
dk_cases <-
  dk_case_data_raw %>%
  select(case_filename:case_text_raw, case_document_category)

saveRDS(dk_cases, here::here("data", "dk_cases.rds"))

## Judge data
dk_case_judges <-
  dk_case_data_raw %>%
  filter(!is.na(case_judges)) %>%
  .$case_judges %>%
  bind_rows() %>%
  unnest(c(judge_clean, judge_order_mentioned)) %>%
  select(-judge_name_raw)

saveRDS(dk_case_judges, here::here("data", "dk_case_judges.rds"))


## Parts data
dk_case_parts <-
  dk_case_data_raw %>%
  filter(!is.na(case_parts)) %>%
  .$case_parts %>%
  bind_rows()

saveRDS(dk_case_parts, here::here("data", "dk_case_parts.rds"))

## Citations data
dk_case_citations  <-
  dk_case_data_raw %>%
  filter(!is.na(case_citations)) %>%
  select(case_filename, case_id, case_citations) %>%
  mutate(case_citations = map(case_citations, as.character)) %>%
  unnest(case_citations)

saveRDS(dk_case_parts, here::here("data", "dk_case_parts.rds"))

## Topic label data
dk_case_topics <-
  dk_case_data_raw %>%
  select(case_filename, case_id, matches("case_topic_")) %>%
  mutate_at(vars(case_topic_labels, case_topic_codes), map, as.character) %>%
  unnest(c(case_topic_labels, case_topic_codes))


saveRDS(dk_case_topics, here::here("data", "dk_case_topics.rds"))

## END ---------------------------------------------------------------
