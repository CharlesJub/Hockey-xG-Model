library(tidyverse)
library(caret)
library(pROC)
library(tidymodels)
library(xgboost)
library(janitor)
library(knitr)
library(kableExtra)
# loading in libraries

pbp_data <- pbp_data <- read_csv("C:/Users/charl/OneDrive/Desktop/Hockey_xG/Hockey-xG-Model/pbp_data.csv", col_select = c(!1))
# Load data from CSV

pbp_data$game_type <- substr(pbp_data$game_id,5,6)
# Create game_type to determine if the game is Pre-season, Regular, or Playoffs

regular_season <- pbp_data %>%
  filter(game_type == "02",
         game_period < 5,
         (empty_net == FALSE | is.na(empty_net)))
# Get data for all regular season plays that weren't on an empty net or in the shootout

regular_season$home_skaters <- rowSums(!is.na(regular_season[,21:26]))
regular_season$away_skaters <- rowSums(!is.na(regular_season[,27:32]))
# Create int values for the number of skaters on the ice

fenwick_events <- c("MISSED_SHOT", "GOAL", "SHOT")
# Create fenwick_events vector to sort by later

regular_season <- 
  regular_season %>% 
  mutate(is_home = ifelse(event_team == home_team, 1 , 0), # Variable for if the team is at home
         is_goal = ifelse(event_type == "GOAL", 1, 0), # Variable for if the event is a goal
         is_shot = if_else(event_type %in% fenwick_events, 1, 0), # Variable for if the event is a fenwick event
         time_diff = if_else(event_type == "FACEOFF", NA, game_seconds - lag(game_seconds)), # Var for difference in time between event and prev event
         dist_diff = if_else(event_type == "FACEOFF", NA, sqrt((coords_x - lag(coords_x)) ^ 2 + (coords_y - lag(coords_y)) ^ 2)), # Var for difference in distance between play and prev play
         dist_per_time = dist_diff / time_diff, # var for speed between plays
         is_rebound_0_sec = ifelse(time_diff == 0 & event_type %in% fenwick_events &
                                     event_team == lag(event_team) & 
                                     lag(event_type) %in% fenwick_events, 1, 0), # Var for if rebound is 0 seconds after original shot
         is_rebound_1_sec = ifelse(time_diff == 1 & event_type %in% fenwick_events &
                                     event_team == lag(event_team) & 
                                     lag(event_type) %in% fenwick_events, 1, 0), # Var for if rebound is 1 second after original shot
         is_rebound_2_sec = ifelse(time_diff == 2 & event_type %in% fenwick_events &
                                     event_team == lag(event_team) & 
                                     lag(event_type) %in% fenwick_events, 1, 0), # Var for if rebound is 2 second after original shot
         is_rebound_3_sec = ifelse(time_diff == 3 & event_type %in% fenwick_events &
                                     event_team == lag(event_team) & 
                                     lag(event_type) %in% fenwick_events, 1, 0), # Var for if rebound is 3 second after original shot
         is_rush = ifelse(time_diff < 4 &
                            lag(abs(coords_x)) < 25 &
                            event_type %in% fenwick_events,
                          1, 0), # Var for if shot is rush
         last_event = lag(event_type), # Var for prev event type
         last_event_team_is_home = lag(is_home), # Var for prev event team
         last_event_same_team = if_else(last_event_team_is_home == is_home, 1, 0), # Var for if last event was same team
         game_state = paste0(case_when(
           home_skaters < 3 ~ "E",
           home_skaters == 3 ~ "3",
           home_skaters == 4 ~ "4",
           home_skaters == 5 ~ "5",
           home_skaters == 6 ~ "6",
           home_skaters > 6 ~ "E"
         ), "v", case_when(
           away_skaters < 3 ~ "E",
           away_skaters == 3 ~ "3",
           away_skaters == 4 ~ "4",
           away_skaters == 5 ~ "5",
           away_skaters == 6 ~ "6",
           away_skaters > 6 ~ "E"
         )), # Var for game state encoded 
         shooter_strength = if_else(
           event_team == home_team,
           case_when(game_state %in% c("5v5","4v4","3v3") ~ "Even",
                     game_state %in% c("6v5","6v4","5v4","5v3","4v3") ~ "PowerPlay",
                     game_state %in% c("4v5","3v5","3v4","5v6","4v6") ~ "ShortHanded",
                     TRUE ~ "Other"),
           case_when(game_state %in% c("5v5","4v4","3v3") ~ "Even",
                     game_state %in% c("5v6","4v6","4v5","3v5","3v4") ~ "PowerPlay",
                     game_state %in% c("5v4","5v3","4v3","6v5","6v4") ~ "ShortHanded",
                     TRUE ~ "Other")
         ) , # Var for game state
         coords_y = ifelse(coords_x < 0, -1 * coords_y, coords_y), # Flip y for shots on one side of ice
         coords_x = abs(coords_x), # Make all shots on one goal
         shot_angle = (asin(abs(coords_y)/sqrt((87.95 - abs(coords_x))^2 + coords_y^2))*180)/ 3.14, # Var for angle of shot
         shot_angle = ifelse(abs(coords_x) > 88, 90 + (180-(90 + shot_angle)), # Make shots behind net have >90 degree shot angle
                            shot_angle),
         distance = sqrt((87.95 - abs(coords_x))^2 + coords_y^2), # Distance from center of goal
         skater_team = if_else(is_home == 1, home_skaters, away_skaters), # Num of skaters on ice for shooter
         skater_op = if_else(is_home == 0, home_skaters, away_skaters),  # Num of skaters on ice against shooter
         high_danger = if_else((8 > coords_y & coords_y > -8) &
                                 (coords_x > 89 & coords_x < 59), 1, 0),  # is the shot in the high danger zone
         prev_event_team_take = if_else(event_team == lag(event_team) & lag(event_type) == "TAKEAWAY", 1, 0), # Was last event takeaway by shooter team
         prev_event_opp_give = if_else(event_team != lag(event_team) & lag(event_type) == "GIVEAWAY", 1, 0), # Was last event giveaway by opp team
         prev_event_opp_block = if_else(event_team != lag(event_team) & lag(event_type) == "BLOCKED_SHOT", 1, 0) # Was last event block by opp team
         
  )



fenwick_data <- regular_season %>% filter(event_type %in% fenwick_events)
# Filter out non-fenwick events

fenwick_data$shot_type <-  sapply(fenwick_data$long_description, function(x) strsplit(x, ",")[[1]][2])
# Create Var for shot type

fenwick_data <- fenwick_data %>% filter(!is.na(shot_type) & !is.na(dist_diff) &
                                          !is.na(shooter_strength) &
                                          3<=away_skaters & away_skaters<=6 &
                                            3<=home_skaters & home_skaters<=6)
# Filter out null values and events with impossible skater numbers

# fenwick_data <- fenwick_data %>% filter(shooter_strength =="Even")

fenwick_data$is_goal <- as.factor(fenwick_data$is_goal)
# Factorize target value

formula_model <- is_goal ~ shot_type+time_diff+dist_diff+dist_per_time+is_rush+last_event+coords_x+coords_y+
  skater_team+skater_op+last_event_same_team+is_rebound_0_sec+is_rebound_1_sec+is_rebound_2_sec+
  is_rebound_3_sec+prev_event_team_take+prev_event_opp_give+prev_event_opp_block+game_period+game_seconds
# Create Formula

set.seed(1234)


data_split <- initial_split(fenwick_data, prop = 0.7, strata = is_goal)
train_data <- training(data_split)
test_data <- testing(data_split)
# Test/Train Split

recipe <- recipe(formula_model,
                 data = train_data) %>%
  step_dummy(all_nominal_predictors()) %>% 
  step_zv(all_numeric()) %>% 
  step_normalize(all_numeric(), -all_outcomes()) %>% 
  prep()
# One-hot and normalization

recipe

xg_model <- boost_tree(tree_depth = 7, trees = 22) %>%
  set_engine("xgboost") %>% 
  set_mode("classification")
# Initialize xgboost

set.seed(123)
folds <- vfold_cv(juice(recipe), strata = is_goal)
# Cross fold validation

set.seed(234)
xg_res <- xg_model %>% 
  fit_resamples(is_goal ~ .,
                folds,
                control = control_resamples(save_pred = TRUE))

xg_res %>% collect_metrics()
# View Metrics for folds

workflow <- workflow() %>%
  add_recipe(recipe) %>%
  add_model(xg_model)

model_fit <- workflow %>% 
  fit(train_data)
# Fit Model
model_fit


test_data$xG <- predict(model_fit, new_data = test_data, type = "prob")$.pred_1

g <- roc(is_goal ~ xG, data = test_data)
plot(g)
auc(g)
# Create AUC plot and print AUc value

fenwick_data$xG <- predict(model_fit, new_data = fenwick_data, type = "prob")$.pred_1
full_data <- fenwick_data %>%  mutate(is_goal = as.numeric(levels(is_goal))[is_goal])
avg_xG_by_coord <- full_data %>% group_by(coords_x, coords_y) %>%
  summarise(xg = mean(xG))
# Apply model to all data

ggplot(avg_xG_by_coord, aes(coords_x, coords_y, fill = xg)) + geom_raster() +
  scale_fill_gradient(low = 'blue', high = 'red')+
  geom_vline(xintercept = 0, color = 'red') +
  geom_vline(xintercept = 30, color = 'blue') +
  geom_vline(xintercept = 90, color = 'red') +
  xlab('X Coordinates') + ylab('Y Coordinates') +
  labs(title = 'Average xG Value by Coordinate')
# Graph xG by coordinate 
full_data %>%
  group_by(event_player_1, event_team) %>%
  summarise( xG = round(sum(xG),3), Goals = sum(is_goal), Difference = round(sum(xG) - sum(is_goal),3)) %>% 
  arrange(desc(xG)) %>% head(10) %>% gt(groupname_col = TRUE) %>% cols_label(event_player_1 = "Player", 
                                                                             event_team = "Team")
# Make pivot table for xG for each player
full_data %>%
  group_by(event_team) %>%
  summarise( xG = round(sum(xG), 3), Goals = sum(is_goal), Difference = round(sum(xG) - sum(is_goal),3)) %>%
  arrange(desc(xG)) %>% head(10) %>% gt(groupname_col = TRUE) %>% cols_label(event_team = "Team") 
# Make pivot table for xG for each team
