#Data import
if(!dir.exists(file.path("data", "mp02"))){
  dir.create(file.path("data", "mp02"), showWarnings=FALSE, recursive=TRUE)
}

library <- function(pkg){
  ## Mask base::library() to automatically install packages if needed
  ## Masking is important here so downlit picks up packages and links
  ## to documentation
  pkg <- as.character(substitute(pkg))
  options(repos = c(CRAN = "https://cloud.r-project.org"))
  if(!require(pkg, character.only=TRUE, quietly=TRUE)) install.packages(pkg)
  stopifnot(require(pkg, character.only=TRUE, quietly=TRUE))
}

library(tidyverse)
library(glue)
library(readxl)
library(tidycensus)

get_acs_all_years <- function(variable, geography="cbsa",
                              start_year=2009, end_year=2023){
  fname <- glue("{variable}_{geography}_{start_year}_{end_year}.csv")
  fname <- file.path("data", "mp02", fname)
  
  if(!file.exists(fname)){
    YEARS <- seq(start_year, end_year)
    YEARS <- YEARS[YEARS != 2020] # Drop 2020 - No survey (covid)
    
    ALL_DATA <- map(YEARS, function(yy){
      tidycensus::get_acs(geography, variable, year=yy, survey="acs1") |>
        mutate(year=yy) |>
        select(-moe, -variable) |>
        rename(!!variable := estimate)
    }) |> bind_rows()
    
    write_csv(ALL_DATA, fname)
  }
  
  read_csv(fname, show_col_types=FALSE)
}

# Household income (12 month)
INCOME <- get_acs_all_years("B19013_001") |>
  rename(household_income = B19013_001)

# Monthly rent
RENT <- get_acs_all_years("B25064_001") |>
  rename(monthly_rent = B25064_001)

# Total population
POPULATION <- get_acs_all_years("B01003_001") |>
  rename(population = B01003_001)

# Total number of households
HOUSEHOLDS <- get_acs_all_years("B11001_001") |>
  rename(households = B11001_001)

# Number of new housing units built each year
get_building_permits <- function(start_year = 2009, end_year = 2023){
fname <- glue("housing_units_{start_year}_{end_year}.csv")
fname <- file.path("data", "mp02", fname)

if(!file.exists(fname)){
  HISTORICAL_YEARS <- seq(start_year, 2018)
  
  HISTORICAL_DATA <- map(HISTORICAL_YEARS, function(yy){
    historical_url <- glue("https://www.census.gov/construction/bps/txt/tb3u{yy}.txt")
    
    LINES <- readLines(historical_url)[-c(1:11)]
    
    CBSA_LINES <- str_detect(LINES, "^[[:digit:]]")
    CBSA <- as.integer(str_sub(LINES[CBSA_LINES], 5, 10))
    
    PERMIT_LINES <- str_detect(str_sub(LINES, 48, 53), "[[:digit:]]")
    PERMITS <- as.integer(str_sub(LINES[PERMIT_LINES], 48, 53))
    
    data_frame(CBSA = CBSA,
               new_housing_units_permitted = PERMITS, 
               year = yy)
  }) |> bind_rows()
  
  CURRENT_YEARS <- seq(2019, end_year)
  
  CURRENT_DATA <- map(CURRENT_YEARS, function(yy){
    current_url <- glue("https://www.census.gov/construction/bps/xls/msaannual_{yy}99.xls")
    
    temp <- tempfile()
    
    download.file(current_url, destfile = temp, mode="wb")
    
    fallback <- function(.f1, .f2){
      function(...){
        tryCatch(.f1(...), 
                 error=function(e) .f2(...))
      }
    }
    
    reader <- fallback(read_xlsx, read_xls)
    
    reader(temp, skip=5) |>
      na.omit() |>
      select(CBSA, Total) |>
      mutate(year = yy) |>
      rename(new_housing_units_permitted = Total)
  }) |> bind_rows()
  
  ALL_DATA <- rbind(HISTORICAL_DATA, CURRENT_DATA)
  
  write_csv(ALL_DATA, fname)
  
}

read_csv(fname, show_col_types=FALSE)
}

PERMITS <- get_building_permits()

# Latest NAICS data schema 
library(httr2)
library(rvest)
get_bls_industry_codes <- function(){
  fname <- file.path("data", "mp02", "bls_industry_codes.csv")
  library(dplyr)
  library(tidyr)
  library(readr)
  
  if(!file.exists(fname)){
    
    resp <- request("https://www.bls.gov") |> 
      req_url_path("cew", "classifications", "industry", "industry-titles.htm") |>
      req_headers(`User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:143.0) Gecko/20100101 Firefox/143.0") |> 
      req_error(is_error = \(resp) FALSE) |>
      req_perform()
    
    resp_check_status(resp)
    
    naics_table <- resp_body_html(resp) |>
      html_element("#naics_titles") |> 
      html_table() |>
      mutate(title = str_trim(str_remove(str_remove(`Industry Title`, Code), "NAICS"))) |>
      select(-`Industry Title`) |>
      mutate(depth = if_else(nchar(Code) <= 5, nchar(Code) - 1, NA)) |>
      filter(!is.na(depth))
    
    # These were looked up manually on bls.gov after finding 
    # they were presented as ranges. Since there are only three
    # it was easier to manually handle than to special-case everything else
    naics_missing <- tibble::tribble(
      ~Code, ~title, ~depth, 
      "31", "Manufacturing", 1,
      "32", "Manufacturing", 1,
      "33", "Manufacturing", 1,
      "44", "Retail", 1, 
      "45", "Retail", 1,
      "48", "Transportation and Warehousing", 1, 
      "49", "Transportation and Warehousing", 1
    )
    
    naics_table <- bind_rows(naics_table, naics_missing)
    
    naics_table <- naics_table |> 
      filter(depth == 4) |> 
      rename(level4_title=title) |> 
      mutate(level1_code = str_sub(Code, end=2), 
             level2_code = str_sub(Code, end=3), 
             level3_code = str_sub(Code, end=4)) |>
      left_join(naics_table, join_by(level1_code == Code)) |>
      rename(level1_title=title) |>
      left_join(naics_table, join_by(level2_code == Code)) |>
      rename(level2_title=title) |>
      left_join(naics_table, join_by(level3_code == Code)) |>
      rename(level3_title=title) |>
      select(-starts_with("depth")) |>
      rename(level4_code = Code) |>
      select(level1_title, level2_title, level3_title, level4_title, 
             level1_code,  level2_code,  level3_code,  level4_code) |>
      drop_na() |>
      mutate(across(contains("code"), as.integer))
    
    write_csv(naics_table, fname)
  }
  
  read_csv(fname, show_col_types=FALSE)
}

INDUSTRY_CODES <- get_bls_industry_codes()

# BLS Quarterly Census of Employment and Wages
library(httr2)
library(rvest)
get_bls_qcew_annual_averages <- function(start_year=2009, end_year=2023){
  fname <- glue("bls_qcew_{start_year}_{end_year}.csv.gz")
  fname <- file.path("data", "mp02", fname)
  
  YEARS <- seq(start_year, end_year)
  YEARS <- YEARS[YEARS != 2020] # Drop Covid year to match ACS
  
  if(!file.exists(fname)){
    ALL_DATA <- map(YEARS, .progress=TRUE, possibly(function(yy){
      fname_inner <- file.path("data", "mp02", glue("{yy}_qcew_annual_singlefile.zip"))
      
      if(!file.exists(fname_inner)){
        request("https://www.bls.gov") |> 
          req_url_path("cew", "data", "files", yy, "csv",
                       glue("{yy}_annual_singlefile.zip")) |>
          req_headers(`User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:143.0) Gecko/20100101 Firefox/143.0") |> 
          req_retry(max_tries=5) |>
          req_perform(fname_inner)
      }
      
      if(file.info(fname_inner)$size < 755e5){
        warning(sQuote(fname_inner), "appears corrupted. Please delete and retry this step.")
      }
      
      read_csv(fname_inner, 
               show_col_types=FALSE) |> 
        mutate(YEAR = yy) |>
        select(area_fips, 
               industry_code, 
               annual_avg_emplvl, 
               total_annual_wages, 
               YEAR) |>
        filter(nchar(industry_code) <= 5, 
               str_starts(area_fips, "C")) |>
        filter(str_detect(industry_code, "-", negate=TRUE)) |>
        mutate(FIPS = area_fips, 
               INDUSTRY = as.integer(industry_code), 
               EMPLOYMENT = as.integer(annual_avg_emplvl), 
               TOTAL_WAGES = total_annual_wages) |>
        select(-area_fips, 
               -industry_code, 
               -annual_avg_emplvl, 
               -total_annual_wages) |>
        # 10 is a special value: "all industries" , so omit
        filter(INDUSTRY != 10) |> 
        mutate(AVG_WAGE = TOTAL_WAGES / EMPLOYMENT)
    })) |> bind_rows()
    
    write_csv(ALL_DATA, fname)
  }
  
  ALL_DATA <- read_csv(fname, show_col_types=FALSE)
  
  ALL_DATA_YEARS <- unique(ALL_DATA$YEAR)
  
  YEARS_DIFF <- setdiff(YEARS, ALL_DATA_YEARS)
  
  if(length(YEARS_DIFF) > 0){
    stop("Download failed for the following years: ", YEARS_DIFF, 
         ". Please delete intermediate files and try again.")
  }
  
  ALL_DATA
}

WAGES <- get_bls_qcew_annual_averages()
# ---- 1. Explore Loaded Data ----

glimpse(HOUSEHOLDS)
glimpse(INCOME)
glimpse(RENT)
glimpse(POPULATION)
glimpse(PERMITS)
glimpse(WAGES)

# ---- 2. Clean and Standardize Column Names ----

HOUSEHOLDS <- HOUSEHOLDS %>% rename_with(tolower)
INCOME <- INCOME %>% rename_with(tolower)
RENT <- RENT %>% rename_with(tolower)
POPULATION <- POPULATION %>% rename_with(tolower)
PERMITS <- PERMITS %>% rename_with(tolower)
WAGES <- WAGES %>% rename_with(tolower)


# ---- 3. Align join key names ----

PERMITS <- PERMITS %>% rename(geoid = cbsa)

# ---- Task 2: Multi-Table Question 1 ----
# Which CBSA permitted the largest number of new housing units (2010–2019)?

permits_decade <- PERMITS %>%
  filter(year >= 2010, year <= 2019) %>%
  group_by(geoid) %>%
  summarise(total_permits_2010_2019 = sum(new_housing_units_permitted, na.rm = TRUE)) %>%
  arrange(desc(total_permits_2010_2019)) %>%
  left_join(select(HOUSEHOLDS, geoid, name), by = "geoid")

head(permits_decade, 5)

# ---- Task 2: Multi-Table Question 2 (exclude 2020) ----
# In what year did Albuquerque, NM (CBSA 10740) permit the most new housing units?

albuquerque_peak <- PERMITS %>%
  filter(geoid == 10740, year != 2020) %>%       # exclude 2020 due to COVID data gap
  group_by(year) %>%
  summarise(total_permits = sum(new_housing_units_permitted, na.rm = TRUE)) %>%
  arrange(desc(total_permits))

head(albuquerque_peak, 3)

# ---- Task 2: Multi-Table Question 3 (part 1) ----
# Compute total income per CBSA and prepare for state-level aggregation

income_state <- INCOME %>%
  filter(year == 2015) %>%                                  # focus on 2015
  left_join(HOUSEHOLDS %>% filter(year == 2015), 
            by = c("geoid", "name", "year")) %>%            # align households
  mutate(total_income_cbsa = household_income * households)  # weighted total

glimpse(income_state)

# ---- Task 2: Multi-Table Question 3 (part 2) ----
# Extract principal state abbreviation from CBSA names
# Some CBSAs span multiple states; we only take the first listed

income_state <- income_state %>%
  mutate(state = str_extract(name, ", (.{2})")) %>% 
  mutate(state = str_remove(state, ", "))  # clean the comma

state_income_2015 <- income_state %>%
  left_join(POPULATION %>% filter(year == 2015), 
            by = c("geoid", "name", "year")) %>%
  group_by(state) %>%
  summarise(
    total_income_state = sum(total_income_cbsa, na.rm = TRUE),
    total_population_state = sum(population, na.rm = TRUE),
    avg_individual_income = total_income_state / total_population_state
  ) %>%
  arrange(desc(avg_individual_income))

head(state_income_2015, 5)

# ---- Task 2: Multi-Table Question 4 (part 1) ----
# Convert BLS FIPS codes (e.g., "C1074") to numeric GEOID format (e.g., 10740)

WAGES <- WAGES %>%
  mutate(
    geoid_bls = fips %>%
      str_remove("C") %>%       # remove the "C" prefix
      as.double() * 10          # multiply by 10 to match Census GEOID format
  )
# ---- Task 2: Multi-Table Question 4 (part 2) ----
# Identify when NYC last had the highest number of data scientists (NAICS 5182)

data_scientists <- WAGES %>%
  filter(industry == 5182) %>%               # focus on data science/analytics
  group_by(geoid_bls, year) %>%
  summarise(total_employment = sum(employment, na.rm = TRUE)) %>%
  arrange(year, desc(total_employment))

# Find which CBSA leads per year
leaders_by_year <- data_scientists %>%
  group_by(year) %>%
  slice_max(total_employment, n = 1)

# Inspect NYC pattern
leaders_by_year %>%
  filter(geoid_bls == 35620) %>%
  arrange(desc(year)) %>%
  head(10)

# ---- Task 2: Multi-Table Question 5 ----
# Fraction of total wages in NYC (CBSA 35620) from Finance & Insurance (NAICS 52)

finance_share_nyc <- WAGES %>%
  filter(geoid_bls == 35620) %>%
  group_by(year) %>%
  summarise(
    total_wages_all = sum(total_wages, na.rm = TRUE),
    total_wages_finance = sum(total_wages[industry == 52], na.rm = TRUE)
  ) %>%
  mutate(finance_share = total_wages_finance / total_wages_all) %>%
  arrange(desc(finance_share))

head(finance_share_nyc, 5)

# ---- Task 3, Plot 1: Rent vs Household Income (2009) ----
library(ggplot2)
library(scales)
library(dplyr)

rent_income_2009 <- INCOME %>%
  filter(year == 2009) %>%
  select(geoid, name, household_income) %>%
  inner_join(
    RENT %>% filter(year == 2009) %>% select(geoid, monthly_rent),
    by = "geoid"
  ) %>%
  filter(!is.na(household_income), !is.na(monthly_rent))

p_rent_income_2009 <- ggplot(rent_income_2009,
                             aes(x = household_income, y = monthly_rent)) +
  geom_point(alpha = 0.6) +
  geom_smooth(method = "lm", se = FALSE, linewidth = 0.7) +
  labs(
    title = "Monthly Rent vs. Household Income by CBSA (2009)",
    subtitle = "ACS 1-year estimates; each point is a CBSA",
    x = "Median Household Income (USD)",
    y = "Median Monthly Rent (USD)",
    caption = "Source: U.S. Census Bureau, ACS 1-year (2009)"
  ) +
  scale_x_continuous(labels = label_dollar(accuracy = 1)) +
  scale_y_continuous(labels = label_dollar(accuracy = 1)) +
  theme_bw(base_size = 12)

print(p_rent_income_2009)

# ---- Task 3, Plot 2: Total vs Health-sector Employment (evolution over time) ----
library(ggplot2)
library(scales)
library(dplyr)
library(stringr)

# 1) Total employment per CBSA-year (all industries)
total_emp <- WAGES %>%
  group_by(geoid_bls, year) %>%
  summarise(total_emp = sum(employment, na.rm = TRUE), .groups = "drop")

# 2) Health sector (NAICS 62*) employment per CBSA-year
# Include all industries whose NAICS code starts with "62" (62, 621, 622, 623, 624, ...)
health_emp <- WAGES %>%
  mutate(industry_chr = as.character(industry)) %>%
  filter(str_starts(industry_chr, "62")) %>%
  group_by(geoid_bls, year) %>%
  summarise(health_emp = sum(employment, na.rm = TRUE), .groups = "drop")

# 3) Join, and create coarse time periods to make evolution readable
emp_df <- total_emp %>%
  inner_join(health_emp, by = c("geoid_bls", "year")) %>%
  filter(!is.na(total_emp), !is.na(health_emp)) %>%
  mutate(period = case_when(
    year <= 2012 ~ "2009–2012",
    year <= 2016 ~ "2013–2016",
    TRUE         ~ "2017–2023"
  )) %>%
  mutate(period = factor(period, levels = c("2009–2012","2013–2016","2017–2023")))

# 4) Publication-ready plot (small multiples to show evolution)
p_emp_health <- ggplot(emp_df, aes(x = total_emp, y = health_emp)) +
  geom_point(alpha = 0.5) +
  facet_wrap(~ period) +
  labs(
    title    = "Health-Sector Employment vs Total Employment by CBSA",
    subtitle = "Evolution over time shown via small multiples (NAICS 62*)",
    x        = "Total Employment (All Industries)",
    y        = "Employment in Health Care & Social Assistance (NAICS 62*)",
    caption  = "Source: BLS QCEW; points are CBSA-years; facets indicate periods"
  ) +
  scale_x_continuous(labels = scales::label_number(scale_cut = scales::cut_short_scale())) +
  scale_y_continuous(labels = scales::label_number(scale_cut = scales::cut_short_scale())) +
  theme_bw(base_size = 12)

print(p_emp_health)

# ---- Task 3, Plot 3: Average Household Size over Time (Extra Credit) ----
# Packages
library(ggplot2)
library(dplyr)
if (!requireNamespace("gghighlight", quietly = TRUE)) install.packages("gghighlight")
library(gghighlight)

# Compute average household size: population / households
household_size <- POPULATION %>%
  select(geoid, name, year, population) %>%
  inner_join(HOUSEHOLDS %>% select(geoid, year, households),
             by = c("geoid", "year")) %>%
  mutate(avg_household_size = population / households) %>%
  filter(year >= 2009, year != 2020)                       # omit COVID gap

# Target CBSAs to highlight
highlight_targets <- c(
  "New York-Newark-Jersey City, NY-NJ-PA Metro Area",
  "Los Angeles-Long Beach-Anaheim, CA Metro Area"
)

# Spaghetti plot with highlights and direct labels
p_household_size <- ggplot(household_size,
                           aes(x = year, y = avg_household_size,
                               group = name, color = name)) +
  geom_line(alpha = 0.35, linewidth = 0.6) +
  gghighlight(name %in% highlight_targets,
              label_key = name,
              use_direct_label = TRUE,
              label_params = list(size = 3, fontface = "bold")) +
  labs(
    title    = "Average Household Size Over Time by CBSA (2009–2023)",
    x        = "Year",
    y        = "Average Household Size",
    caption  = "Source: U.S. Census Bureau, ACS 1-year; 2020 omitted"
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "none")

print(p_household_size)

# ---- Task 4: Rent Burden Metric (Population-Weighted Version) ----

# 1) Join RENT, INCOME, POPULATION 
rent_burden_weighted <- RENT %>%
  inner_join(INCOME,    by = c("geoid", "name", "year")) %>%
  inner_join(POPULATION, by = c("geoid", "name", "year")) %>%
  filter(!is.na(monthly_rent), !is.na(household_income), household_income > 0) %>%
  mutate(
    rent_to_income = (monthly_rent * 12) / household_income
  )

# 2) Population-weighted baseline (100 = national average)
baseline_rent_ratio_pop <- with(
  rent_burden_weighted,
  weighted.mean(rent_to_income, w = population, na.rm = TRUE)
)

# 3) Standardized index (weighted)
rent_burden_weighted <- rent_burden_weighted %>%
  mutate(
    rent_burden_index_weighted = (rent_to_income / baseline_rent_ratio_pop) * 100
  )

# 4) (Optional) Compare to unweighted baseline if available
if (exists("baseline_rent_ratio")) {
  compare_baselines <- data.frame(
    baseline_unweighted = baseline_rent_ratio,
    baseline_weighted   = baseline_rent_ratio_pop
  )
  print(compare_baselines)
} else {
  message("Unweighted baseline not found in this session; skipping baseline comparison.")
}

# 5) Latest year weighted rankings
latest_year <- max(rent_burden_weighted$year, na.rm = TRUE)

rent_burden_latest_weighted <- rent_burden_weighted %>%
  filter(year == latest_year) %>%
  distinct(name, .keep_all = TRUE) %>%  # ensure one row per CBSA
  select(name, rent_burden_index_weighted) %>%
  arrange(desc(rent_burden_index_weighted))

top10_burdened_weighted    <- head(rent_burden_latest_weighted, 10)
bottom10_burdened_weighted <- tail(rent_burden_latest_weighted, 10)

cat("\nTop 10 Most Burdened CBSAs (Weighted):\n")
print(top10_burdened_weighted)

cat("\nBottom 10 Least Burdened CBSAs (Weighted):\n")
print(bottom10_burdened_weighted)

# 6) Quick peek at a few high-burden CBSAs (weighted)
compare_sample <- rent_burden_weighted %>%
  filter(year == latest_year) %>%
  select(name, rent_burden_index_weighted) %>%
  arrange(desc(rent_burden_index_weighted)) %>%
  head(10)

cat("\nTop 10 snapshot (Weighted):\n")
print(compare_sample)

# ---- Task 4 Tables: DT outputs for trend + top/bottom ----
suppressPackageStartupMessages({ library(dplyr); library(DT) })

# Pick ONE metro to showcase the time trend (change this string as needed)
METRO_PICK <- "Pittsburgh, PA Metro Area"

# A) Trend table for selected metro
rent_burden_trend_dt <- rent_burden_weighted %>%
  filter(name == METRO_PICK) %>%
  arrange(year) %>%
  transmute(
    year,
    monthly_rent              = round(monthly_rent, 0),
    household_income          = round(household_income, 0),
    rent_to_income            = round(rent_to_income, 3),
    rent_burden_index_weighted= round(rent_burden_index_weighted, 1)
  )

# B) Latest-year top & bottom CBSAs by weighted index
latest_year_rb <- max(rent_burden_weighted$year, na.rm = TRUE)

rent_burden_latest_w <- rent_burden_weighted %>%
  filter(year == latest_year_rb) %>%
  distinct(name, .keep_all = TRUE) %>%
  transmute(
    name,
    rent_burden_index_weighted = round(rent_burden_index_weighted, 1),
    rent_to_income = round(rent_to_income, 3)
  ) %>%
  arrange(desc(rent_burden_index_weighted))

top10_rent_burden_w    <- head(rent_burden_latest_w, 10)
bottom10_rent_burden_w <- tail(rent_burden_latest_w, 10)

# ---- DT render (interactive; shows in Viewer/HTML) ----
if (interactive()) {
  DT::datatable(
    rent_burden_trend_dt,
    caption = paste("Rent Burden Trend —", METRO_PICK),
    options = list(pageLength = 12, dom = "tip"),
    rownames = FALSE
  )
  
  DT::datatable(
    top10_rent_burden_w,
    caption = paste0("Top 10 Most Burdened CBSAs (Weighted, ", latest_year_rb, ")"),
    options = list(pageLength = 10, dom = "tip"),
    rownames = FALSE
  )
  
  DT::datatable(
    bottom10_rent_burden_w,
    caption = paste0("Bottom 10 Least Burdened CBSAs (Weighted, ", latest_year_rb, ")"),
    options = list(pageLength = 10, dom = "tip"),
    rownames = FALSE
  )
}

# Console fallbacks (so you still see something when knitting to PDF)
cat("\nTrend (first 10 rows) —", METRO_PICK, ":\n"); print(head(rent_burden_trend_dt, 10))

# ---- Task 5: Base join + 5-year population lookback ----
library(dplyr)

# Join POPULATION and PERMITS, per CBSA-year
housing_base <- POPULATION %>%
  select(geoid, name, year, population) %>%
  inner_join(
    PERMITS %>% select(geoid, year, new_housing_units_permitted),
    by = c("geoid", "year")
  ) %>%
  arrange(geoid, year) %>%
  group_by(geoid) %>%
  # 5 observed-years lookback (note: 2020 is absent in the data, so lag(5) still spans ~5 years)
  mutate(
    pop_lag5       = dplyr::lag(population, 5),
    pop_growth_5y  = population - pop_lag5,                  # absolute growth over ~5 years
    pop_growth_pct = ifelse(!is.na(pop_lag5) & pop_lag5 > 0,
                            (population / pop_lag5) - 1, NA)  # percentage growth over ~5 years
  ) %>%
  ungroup()

# quick check
housing_base %>% 
  select(geoid, name, year, population, new_housing_units_permitted, pop_lag5, pop_growth_5y, pop_growth_pct) %>%
  head(12)

# ---- Task 5a: Instantaneous Housing Growth Index ----
housing_instantaneous <- housing_base %>%
  mutate(permits_per_1000 = (new_housing_units_permitted / population) * 1000)

baseline_inst <- mean(housing_instantaneous$permits_per_1000, na.rm = TRUE)

housing_instantaneous <- housing_instantaneous %>%
  mutate(housing_growth_index_instantaneous = (permits_per_1000 / baseline_inst) * 100)

# preview top/bottom CBSAs (latest year)
latest_year_inst <- max(housing_instantaneous$year, na.rm = TRUE)

top10_growth <- housing_instantaneous %>%
  filter(year == latest_year_inst) %>%
  arrange(desc(housing_growth_index_instantaneous)) %>%
  select(name, housing_growth_index_instantaneous) %>%
  head(10)

bottom10_growth <- housing_instantaneous %>%
  filter(year == latest_year_inst) %>%
  arrange(housing_growth_index_instantaneous) %>%
  select(name, housing_growth_index_instantaneous) %>%
  head(10)

cat("Top 10 CBSAs (Instantaneous Housing Growth):\n")
print(top10_growth)
cat("\nBottom 10 CBSAs:\n")
print(bottom10_growth)


# ---- Task 5b: Rate-Based Housing Growth (national baseline = 100) ----
housing_rate <- housing_base %>%
  filter(!is.na(pop_growth_5y), pop_growth_5y > 0) %>%
  mutate(
    permits_per_growth = new_housing_units_permitted / pop_growth_5y
  )

baseline_rate <- mean(housing_rate$permits_per_growth, na.rm = TRUE)

housing_rate <- housing_rate %>%
  mutate(
    housing_growth_index_rate = (permits_per_growth / baseline_rate) * 100
  )

# Rank CBSAs by latest available year
latest_year_rate <- max(housing_rate$year, na.rm = TRUE)

top10_rate <- housing_rate %>%
  filter(year == latest_year_rate) %>%
  arrange(desc(housing_growth_index_rate)) %>%
  select(name, housing_growth_index_rate) %>%
  head(10)

bottom10_rate <- housing_rate %>%
  filter(year == latest_year_rate) %>%
  arrange(housing_growth_index_rate) %>%
  select(name, housing_growth_index_rate) %>%
  head(10)

cat("Top 10 CBSAs (Rate-Based Housing Growth):\n")
print(top10_rate)
cat("\nBottom 10 CBSAs:\n")
print(bottom10_rate)

# ---- Task 5c: Composite Housing Growth Index ----
housing_composite <- housing_base %>%
  left_join(
    housing_instantaneous %>% select(geoid, year, housing_growth_index_instantaneous),
    by = c("geoid", "year")
  ) %>%
  left_join(
    housing_rate %>% select(geoid, year, housing_growth_index_rate),
    by = c("geoid", "year")
  ) %>%
  mutate(
    composite_housing_growth_index =
      0.5 * housing_growth_index_instantaneous +
      0.5 * housing_growth_index_rate
  )

# ---- Task 5d: Rolling 5-Year Average (Smooth Composite) ----
# Homebuilding is slow—so smooth yearly volatility for a clearer long-term signal.

library(RcppRoll)

housing_composite <- housing_composite %>%
  group_by(geoid) %>%
  arrange(year, .by_group = TRUE) %>%
  mutate(
    composite_5yr_avg = roll_meanr(composite_housing_growth_index, n = 5, fill = NA)
  )

# ---- Task 5e: Identify High / Low CBSAs (Latest Year Available) ----

latest_year_comp <- max(housing_composite$year, na.rm = TRUE)

top10_composite <- housing_composite %>%
  filter(year == latest_year_comp) %>%
  arrange(desc(composite_5yr_avg)) %>%
  select(name, composite_5yr_avg) %>%
  head(10)

bottom10_composite <- housing_composite %>%
  filter(year == latest_year_comp) %>%
  arrange(composite_5yr_avg) %>%
  select(name, composite_5yr_avg) %>%
  head(10)

cat("Top 10 CBSAs (Composite Housing Growth, 5-Year Rolling):\n")
print(top10_composite)
cat("\nBottom 10 CBSAs:\n")
print(bottom10_composite)

# ---- Task 6: Visualizing Rent Burden vs Housing Growth ----
library(dplyr)
library(ggplot2)
library(stringr)
library(scales)

early_years <- 2009:2011
latest_year_all <- min(
  max(rent_burden_weighted$year, na.rm = TRUE),
  max(housing_composite$year, na.rm = TRUE)
)

rb_early <- rent_burden_weighted %>%
  filter(year %in% early_years) %>%
  group_by(geoid, name) %>%
  summarise(rent_burden_early = mean(rent_burden_index_weighted, na.rm = TRUE), .groups = "drop")

rb_latest <- rent_burden_weighted %>%
  filter(year == latest_year_all) %>%
  select(geoid, name, rent_burden_latest = rent_burden_index_weighted)

pop_growth_total <- POPULATION %>%
  group_by(geoid, name) %>%
  summarise(
    pop_2009 = population[year == 2009][1],
    pop_latest = population[year == latest_year_all][1],
    .groups = "drop"
  ) %>%
  mutate(
    pop_growth_abs = pop_latest - pop_2009,
    pop_growth_pct = ifelse(!is.na(pop_2009) & pop_2009 > 0,
                            (pop_latest / pop_2009) - 1, NA_real_)
  )

growth_latest <- housing_composite %>%
  filter(year == latest_year_all) %>%
  select(geoid, name, composite_5yr_avg)

task6_summary <- rb_early %>%
  inner_join(rb_latest, by = c("geoid","name")) %>%
  inner_join(pop_growth_total, by = c("geoid","name")) %>%
  inner_join(growth_latest, by = c("geoid","name")) %>%
  mutate(
    rent_burden_change = rent_burden_latest - rent_burden_early,
    grew_population = pop_growth_abs > 0
  )

# Plot A: Housing Growth vs Change in Rent Burden (polished)
plot_a <- ggplot(task6_summary,
                 aes(x = rent_burden_change, y = composite_5yr_avg,
                     shape = grew_population)) +
  # Baselines
  geom_hline(yintercept = 100, linewidth = 0.3) +
  geom_vline(xintercept = 0,   linewidth = 0.3) +
  # Points
  geom_point(alpha = 0.7) +
  scale_shape_discrete(name = "Population grew") +
  # Labels
  labs(
    title = "Housing Growth vs. Change in Rent Burden",
    subtitle = paste0("Early baseline: ", min(early_years), "–", max(early_years),
                      "; Latest year: ", latest_year_all),
    x = "Change in Rent Burden Index (Latest − Early)  [< 0 = improved affordability]",
    y = "Composite Housing Growth Index (5-year rolling)  [100 = national avg]"
  ) +
  # Focus the y-range so most CBSAs are readable; adjust if needed
  coord_cartesian(ylim = c(0, 400)) +
  scale_y_continuous(labels = function(x) scales::number(x, accuracy = 1)) +
  # Quadrant annotations
  annotate("text", x = -Inf, y = Inf, hjust = -0.05, vjust = 1.2,
           label = "↑ Growth, ↓ Burden (YIMBY success)", size = 3.5) +
  annotate("text", x =  Inf, y =  0,  hjust = 1.05, vjust = -0.6,
           label = "↓ Growth, ↑ Burden (NIMBY pressure)", size = 3.5) +
  theme_bw(base_size = 12)

plot_a

# Plot B: Rent Burden Index Over Time — Selected CBSAs (polished)
focus_cbsa <- c(
  "New York-Newark-Jersey City, NY-NJ-PA Metro Area",
  "Los Angeles-Long Beach-Anaheim, CA Metro Area",
  "Miami-Fort Lauderdale-West Palm Beach, FL Metro Area",
  "Pittsburgh, PA Metro Area",
  "Austin-Round Rock-San Marcos, TX Metro Area"
)

rb_traj <- rent_burden_weighted %>%
  filter(name %in% focus_cbsa) %>%
  arrange(year)

plot_b <- ggplot(rb_traj, aes(x = year, y = rent_burden_index_weighted, color = name)) +
  geom_hline(yintercept = 100, linewidth = 0.3) +   # national average reference
  geom_line(linewidth = 1) +
  labs(
    title = "Rent Burden Index Over Time — Selected CBSAs",
    subtitle = "Index scaled so 100 = population-weighted national average",
    x = "Year", y = "Rent Burden Index (weighted)"
  ) +
  guides(color = guide_legend(title = "CBSA")) +
  theme_minimal(base_size = 13)

plot_b


