library(tidyverse)

pbp_data <- pbp_data <- read_csv("C:/Users/charl/OneDrive/Desktop/Hockey_xG/Hockey-xG-Model/pbp_data.csv", col_select = c(2:18))

pbp_data$game_type <- substr(pbp_data$game_id,5,6)


regular_season <- 
  pbp_data %>%
  filter(game_type == "02") %>% 
  filter(game_period < 5) %>% 
  mutate(shot = if_else(event_type %in% c("MISSED_SHOT", "BLOCKED_SHOT", "GOAL", "SHOT"), 1, 0),
         goal = if_else(event_type == "GOAL", 1, 0)) %>%
  mutate(time_since_prev_event = if_else(event_type == "FACEOFF", NA, game_seconds - lag(game_seconds)),
         distance_from_prev_event = if_else(event_type == "FACEOFF", NA, sqrt((coords_x - lag(coords_x)) ^ 2 + (coords_y - lag(coords_y)) ^ 2)))

  

