if(!dir.exists(file.path("data", "mp01"))){
  dir.create(file.path("data", "mp01"), showWarnings=FALSE, recursive=TRUE)
}

GLOBAL_TOP_10_FILENAME <- file.path("data", "mp01", "global_top10_alltime.csv")

if(!file.exists(GLOBAL_TOP_10_FILENAME)){
  download.file("https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv", 
                destfile=GLOBAL_TOP_10_FILENAME)
}

COUNTRY_TOP_10_FILENAME <- file.path("data", "mp01", "country_top10_alltime.csv")

if(!file.exists(COUNTRY_TOP_10_FILENAME)){
  download.file("https://www.netflix.com/tudum/top10/data/all-weeks-countries.tsv", 
                destfile=COUNTRY_TOP_10_FILENAME)
}
if(!require("tidyverse")) install.packages("tidyverse")
library(readr)
library(dplyr)

GLOBAL_TOP_10 <- read_tsv(GLOBAL_TOP_10_FILENAME)
str(GLOBAL_TOP_10)
glimpse(GLOBAL_TOP_10)

GLOBAL_TOP_10 <- GLOBAL_TOP_10 |>
  mutate(season_title = if_else(season_title == "N/A", NA_character_, season_title))
glimpse(GLOBAL_TOP_10)

COUNTRY_TOP_10 <- read_tsv(
  COUNTRY_TOP_10_FILENAME,
  na = c("N/A")
)
glimpse(COUNTRY_TOP_10)


# Explorartory Data analysis

library(DT)
GLOBAL_TOP_10 |> 
  head(n=20) |>
  datatable(options=list(searching=FALSE, info=FALSE))

library(stringr)
format_titles <- function(df){
  colnames(df) <- str_replace_all(colnames(df), "_", " ") |> str_to_title()
  df
}

GLOBAL_TOP_10 |> 
  format_titles() |>
  head(n=20) |>
  datatable(options=list(searching=FALSE, info=FALSE)) |>
  formatRound(c('Weekly Hours Viewed', 'Weekly Views'))

GLOBAL_TOP_10 |> 
    select(-season_title) |>
    format_titles() |>
    head(n=20) |>
    datatable(options=list(searching=FALSE, info=FALSE)) |>
    formatRound(c('Weekly Hours Viewed', 'Weekly Views'))

GLOBAL_TOP_10 |> 
  mutate(`runtime_(minutes)` = round(60 * runtime)) |>
  select(-season_title, 
         -runtime) |>
  format_titles() |>
  head(n=20) |>
  datatable(options=list(searching=FALSE, info=FALSE)) |>
  formatRound(c('Weekly Hours Viewed', 'Weekly Views'))

#1 How many different countries does Netflix operate in?
  country_count <- COUNTRY_TOP_10 |>
  filter(!is.na(country_name)) |>
  summarise(n_countries = n_distinct(country_name)) |>
  pull(n_countries)

country_count

#2  Non-English films ranked by number of weeks in Global Top 10
non_english_top <- GLOBAL_TOP_10 |>
  filter(category == "Films (Non-English)") |>
  group_by(show_title) |>
  summarise(total_weeks = n(), .groups = "drop") |>
  arrange(desc(total_weeks))

head(non_english_top, 1)

#3 Films (English + Non-English) ranked by runtime
longest_films <- GLOBAL_TOP_10 |>
  filter(category %in% c("Films (English)", "Films (Non-English)"),
         !is.na(runtime)) |>
  group_by(show_title) |>
  summarise(runtime_min = max(runtime, na.rm = TRUE), .groups = "drop") |>
  arrange(desc(runtime_min))

head(longest_films, 1)

#4 For each category, find the program with the most total global viewing hours

top_programs_by_category <- GLOBAL_TOP_10 |>
  group_by(category, show_title) |>
  summarise(total_hours = sum(weekly_hours_viewed, na.rm = TRUE), .groups = "drop_last") |>
  slice_max(order_by = total_hours, n = 1, with_ties = FALSE) |>
  ungroup() |>
  arrange(category)

top_programs_by_category

#5 Longest consecutive Top-10 run for a TV show in a single country

longest_tv_run <- COUNTRY_TOP_10 |>
  filter(!is.na(week), grepl("^TV", category)) |>
  group_by(country_name, show_title) |>
  arrange(week, .by_group = TRUE) |>
  mutate(
    gap_days   = as.integer(week - lag(week)),
    new_streak = if_else(is.na(gap_days) | gap_days != 7L, 1L, 0L),
    streak_id  = cumsum(coalesce(new_streak, 1L))
  ) |>
  group_by(country_name, show_title, streak_id) |>
  summarise(
    streak_weeks = n(),
    start_week   = min(week, na.rm = TRUE),
    end_week     = max(week, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(desc(streak_weeks), desc(end_week)) |>
  slice(1)

#6 Outlier country for service history
longest_tv_run
country_weeks <- COUNTRY_TOP_10 |>
  filter(!is.na(week)) |>
  group_by(country_name) |>
  summarise(
    n_weeks   = n_distinct(week),
    last_week = max(week, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(n_weeks, country_name)

country_weeks_outlier <- slice_head(country_weeks, n = 1)

country_weeks_outlier

#7 Total global viewing hours for "Squid Game" TV show
squid_game_total <- GLOBAL_TOP_10 |>
  filter(grepl("Squid Game", show_title, ignore.case = TRUE),
         grepl("^TV", category)) |>
  summarise(
    total_hours = sum(weekly_hours_viewed, na.rm = TRUE)
  )

squid_game_total

#8 Approximate number of views for "Red Notice" film in 2021

red_notice_views_2021 <- GLOBAL_TOP_10 |>
  filter(show_title == "Red Notice", lubridate::year(week) == 2021) |>
  summarise(
    total_hours_2021 = sum(weekly_hours_viewed, na.rm = TRUE),
    approx_views_2021 = (total_hours_2021 * 60) / 118
  )

red_notice_views_2021

#9 Films that reached #1 in the US but did not debut at #1
films_reach1_after_count <- COUNTRY_TOP_10 |>
  filter(country_name == "United States", grepl("^Films", category)) |>
  group_by(show_title) |>
  summarise(
    debut_week   = min(week, na.rm = TRUE),
    debut_rank   = weekly_rank[which.min(week)],
    ever_rank1   = any(weekly_rank == 1, na.rm = TRUE),
    first_week_1 = if (ever_rank1) min(week[weekly_rank == 1], na.rm = TRUE) else as.Date(NA),
    .groups = "drop"
  ) |>
  filter(ever_rank1, debut_rank != 1) |>
  summarise(n = n()) |>
  pull(n)

films_reach1_after_count

# most recent film to reach #1 in the US but did not debut at #1
most_recent_reach1 <- COUNTRY_TOP_10 |>
  filter(country_name == "United States", grepl("^Films", category)) |>
  group_by(show_title) |>
  summarise(
    debut_week   = min(week, na.rm = TRUE),
    debut_rank   = weekly_rank[which.min(week)],
    ever_rank1   = any(weekly_rank == 1, na.rm = TRUE),
    first_week_1 = if (ever_rank1) min(week[weekly_rank == 1], na.rm = TRUE) else as.Date(NA),
    .groups = "drop"
  ) |>
  filter(ever_rank1, debut_rank != 1) |>
  arrange(desc(first_week_1)) |>
  slice(1)

most_recent_reach1



#10 Most countries hit in the debut week of a TV show/season
q10_debut_reach <- COUNTRY_TOP_10 |>
  filter(grepl("^TV", category)) |>
  group_by(show_title, season_title) |>
  mutate(overall_debut_week = min(week, na.rm = TRUE)) |>
  ungroup() |>
  filter(week == overall_debut_week) |>
  group_by(show_title, season_title, overall_debut_week) |>
  summarise(countries_at_overall_debut = n_distinct(country_name), .groups = "drop") |>
  arrange(desc(countries_at_overall_debut), desc(overall_debut_week)) |>
  slice(1)

q10_debut_reach


# Press release data exploration"Stranger Things"
# # 1. Total global hours viewed across all seasons
stranger_hours <- GLOBAL_TOP_10 |>
  filter(show_title == "Stranger Things") |>
  summarise(total_hours = sum(weekly_hours_viewed, na.rm = TRUE))

stranger_hours

# 2. Total weeks in Top 10 (cumulative across seasons)
stranger_weeks <- GLOBAL_TOP_10 |>
  filter(show_title == "Stranger Things") |>
  summarise(total_weeks = n())

stranger_weeks

# 3. Number of distinct countries where Stranger Things appeared
stranger_countries <- COUNTRY_TOP_10 |>
  filter(show_title == "Stranger Things") |>
  summarise(n_countries = n_distinct(country_name))

stranger_countries

# 4. Comparison: Top English-language TV shows by total hours
top_english_tv <- GLOBAL_TOP_10 |>
  filter(category == "TV (English)") |>
  group_by(show_title) |>
  summarise(total_hours = sum(weekly_hours_viewed, na.rm = TRUE), .groups = "drop") |>
  arrange(desc(total_hours)) |>
  slice_head(n = 5)

top_english_tv

#Press Release: India market
#total viewership by year
india_global_hours <- GLOBAL_TOP_10 |>
filter(show_title %in% (COUNTRY_TOP_10 |> filter(country_name == "India") |> pull(show_title))) |>
  mutate(year = year(week)) |>
  group_by(year) |>
  summarise(total_hours = sum(weekly_hours_viewed, na.rm = TRUE), .groups = "drop") |>
  arrange(year)

india_global_hours

# Unique Titles Watched in India
india_titles_by_year <- COUNTRY_TOP_10 |>
filter(country_name == "India") |>
  mutate(year = year(week)) |>
  group_by(year) |>
  summarise(unique_titles = n_distinct(show_title), .groups = "drop") |>
  arrange(year)

india_titles_by_year

#Multinational Appeal of India’s Favorites
india_multinational_hits <- COUNTRY_TOP_10 |>
  filter(country_name == "India") |>
  distinct(show_title) |>
  inner_join(
    COUNTRY_TOP_10 |>
      group_by(show_title) |>
      summarise(n_countries = n_distinct(country_name), .groups = "drop"),
    by = "show_title"
  ) |>
  arrange(desc(n_countries)) |>
  slice_head(n = 5)

india_multinational_hits

# Press release ; rise of non-english films/shows
non_english_films_by_year <- GLOBAL_TOP_10 |>
  filter(category == "Films (Non-English)") |>
  mutate(year = year(week)) |>
  group_by(year) |>
  summarise(total_hours = sum(weekly_hours_viewed, na.rm = TRUE), .groups = "drop") |>
  arrange(year)

non_english_films_by_year

#english films by year (for comparison)
english_films_by_year <- GLOBAL_TOP_10 |>
  filter(category == "Films (English)") |>
  mutate(year = year(week)) |>
  group_by(year) |>
  summarise(total_hours = sum(weekly_hours_viewed, na.rm = TRUE), .groups = "drop") |>
  arrange(year)

english_films_by_year

