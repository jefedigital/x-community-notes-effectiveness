## import libraries
library(conflicted)
library(corrr)
library(MASS)
library(patchwork)
library(quantreg)
library(ranger)
library(tidyverse)
library(tidymodels)

# prefs
conflict_prefer('filter','dplyr')
conflict_prefer('select','dplyr')
set.seed(40)

# ggplot
custom_theme <- theme_minimal()
theme_set(custom_theme)

# theme(panel.grid.minor = element_blank(), panel.grid.major = element_blank()) 

## Data load

# csv
df_notes <- read_csv('data/df_notes.csv')
df_tweets_sum <- read_csv('data/df_tweets_sum.csv')
df_nicenames <- read_csv('data/nicenames.csv')

# db
# library(duckdb)
# con <- dbConnect(duckdb(), dbdir='/Volumes/seagate_5tb/twitter.duckdb', read_only=TRUE)
# df_notes <- dbGetQuery(con, "select * from analysis.notes_ttp")
# df_tweets_sum <- dbGetQuery(con, "select * from analysis.tweets_all_summary")
# df_tweets_noted_sum <- dbGetQuery(con, "select * from analysis.tweets_noted_summary")
# dbDisconnect(con, shutdown = TRUE)

# save these to file for final paper
# write.csv(df_notes, 'data/df_notes.csv', row.names=FALSE)
# write.csv(df_tweets_sum, 'data/df_tweets_sum.csv', row.names=FALSE)

## Data prep for EDA

# feature select - unused features
# keeping 'nt_publish', 'nt_status_current', 'nr_polar_entropy', 'nt_status_changed', 'nt_publish_revoked', 'nt_publish_revoked_min'

drop_cols = c('nt_id','nt_summary','nt_author_id','nt_ts_created','twt_id','twt_author_id','twt_reply_author_id','twt_convo_id','twt_url','user_created_dt','nt_class_notmis_notoutdated','twt_is_retweet','nr_helpful_empathy','nr_helpful_ctxt_unique','nr_nothelpful_opinion_bias','nr_nothelpful_outdated','nr_nothelpful_offtopic','nt_created_min')

df_notes_all <- df_notes %>% select(-all_of(drop_cols)) 
df_notes_pub <- df_notes %>% filter(nt_publish == 1) %>% select(-all_of(drop_cols)) %>% select(-all_of('nt_publish'))
df_notes_unpub <- df_notes %>% filter(nt_publish == 0) %>% select(-all_of(drop_cols)) %>% select(-all_of('nt_publish'))


# 5.3 EDA

# correlations
c_notes_all <- correlate(df_notes_all)
c_notes_pub <- correlate(df_notes_pub)
c_notes_unpub <- correlate(df_notes_unpub)

c_notes_all %>% shave(upper=FALSE) %>% rplot()
c_notes_pub %>% shave(upper=FALSE) %>% rplot()
c_notes_unpub %>% shave(upper=FALSE) %>% rplot()

metric = 'nt_publish_revoked'
c <- c_notes_all %>% focus(!!metric)
c <- c_notes_pub %>% focus(!!metric)
c <- c_notes_unpub %>% focus(!!metric)



# visualizations

# Hist of ttp values

df_notes_pub %>%
  mutate(ttp_hrs = nt_publish_min/60) %>%
  # filter(ttp_hrs < 500) %>%
  ggplot(aes(x=ttp_hrs)) +
  geom_histogram(bins=30, alpha=0.5, color='black') +
  geom_vline(aes(xintercept = quantile(ttp_hrs, 0.25)), color='grey', linetype='dotted', size=1) +
  geom_vline(aes(xintercept = quantile(ttp_hrs, 0.50)), color='green', linetype='dotted', size=1) +
  geom_vline(aes(xintercept = quantile(ttp_hrs, 0.75)), color='grey', linetype='dotted', size=1) +
  geom_vline(aes(xintercept = quantile(ttp_hrs, 0.82)), color='grey', linetype='dotted', size=1) +
  annotate("text", x = quantile(df_notes_pub$nt_publish_min/60, 0.50), y = 80, label = '50%', vjust = -0.5, hjust = -.25) +
  annotate("text", x = quantile(df_notes_pub$nt_publish_min/60, 0.83), y = 80, label = '83%', vjust = -0.5, hjust = -.25) +
  scale_x_continuous(
    trans = 'log1p',
    labels = scales::label_number(),
    breaks = c(1, 2, 4, 8, 12, 18, 24, 36, 48, 72, 100, 200, 500, 1000, 2000)
  ) +
  labs(title = "Notes - Time-to-Publish Distribution (log)", x = "Hrs", y = "Count") +
  theme(panel.grid.minor = element_blank(), panel.grid.major = element_blank()) 


# Timeseries of ttp values (relation to overall volume)

df_notes_ts <- df_notes %>%
  select(nt_id, nt_ts_created, nt_publish_min, nt_publish) %>%
  mutate(nt_ts_created = as.POSIXct(nt_ts_created)) %>%
  filter(nt_publish == 1)

df_notes_ts %>%
  mutate(date_b = cut(as.Date(nt_ts_created), breaks = "1 days"), 
          ttp_hrs = nt_publish_min/60) %>%
  group_by(date_b) %>%
  summarise(ttp_hrs = mean(ttp_hrs, na.rm = TRUE)) %>%
  mutate(date_b = as.Date(date_b)) %>%
  ggplot(aes(x = date_b, y = ttp_hrs)) +
  geom_col() +
  scale_y_continuous(trans = 'log1p', labels = scales::label_number()) +
  labs(x = "Date (3-day bins)", y = "Avg Time-to-Publish (hrs)")


df_notes %>% 
  mutate(nt_dt_created = as.Date(nt_ts_created)) %>%
  filter(nt_publish == 1) %>%
  select(nt_id, nt_dt_created) %>%
  group_by(nt_dt_created) %>%
  summarize(count = n()) %>%
  ggplot(aes(x = nt_dt_created, y = count)) +
  geom_col() +
  scale_y_continuous(trans = 'log1p', labels = scales::label_number())

df_notes_publish_dt <- df_notes %>% 
  mutate(nt_dt_created = as.Date(nt_ts_created)) %>%
  group_by(nt_dt_created) %>%
  summarize(note_count = n(), notes_published = sum(nt_publish)) %>%
  mutate(pct_publish = round(notes_published/note_count,2)) %>%
  arrange(nt_dt_created)

# CN program essentially on pause entire month of August?
df_notes_publish_dt %>% 
  ggplot(aes(x=nt_dt_created)) + 
  geom_line(aes(y=pct_publish)) +
  geom_col(aes(y=note_count))

scale = 0.0005

df_notes_publish_dt %>%
  mutate(date = as.Date(cut(as.Date(nt_dt_created), breaks = "7 days"))) %>%
  group_by(date) %>%
  summarize(note_count = sum(note_count), notes_published = sum(notes_published)) %>%
  mutate(pct_publish = notes_published / note_count) %>%
  ggplot(aes(x = date)) +
  geom_col(aes(y = note_count), fill = 'grey30', alpha = 0.5) +
  geom_line(aes(y = pct_publish / scale), color = 'orange2', size = 0.5) +
  scale_y_continuous(
    name = "Note Count",
    sec.axis = sec_axis(trans = ~ . * scale, name = "Pct Publish")
  )


stop



# View volume impacted by ttp values (not a corr but sense of % total volume impacted by longer ttps)


# ts of all election tweets by day
df_tweets_sum %>%
  mutate(week = cut(as.Date(twt_dt_created), breaks = "week")) %>%
  mutate(week = as.Date(week)) %>%
  ggplot(aes(x=week, y=tweets_ct)) +
  geom_col() +
  scale_x_date(date_labels = "%b", date_breaks = "1 month") +
  scale_y_continuous(labels = scales::label_number(scale = 1e-6, suffix = "M")) +
  labs(title = '2024 Election-related Posts on X', x = "2024", y = "Post Count")

df_tweets_sum %>%
  mutate(week = cut(as.Date(twt_dt_created), breaks = "week")) %>%
  mutate(week = as.Date(week)) %>%
  ggplot(aes(x=week, y=twt_views)) +
  geom_col() +
  scale_x_date(date_labels = "%b", date_breaks = "1 month") +
  scale_y_continuous(labels = scales::label_number(scale = 1e-6, suffix = "M")) +
  labs(title = '2024 Election-related Post Views on X', x = "2024", y = "Views Count")





stop
# Community Notes are only ever created for a very small percentage (~0.02%) of posts on the platform.


# Of 38.5M election-related posts in this dataset, only 5,551 (0.014%) received one or more notes. (no graph)
sum(df_tweets_sum$tweets_ct, na.rm=TRUE) # 38497220
sum(df_tweets_noted_sum$tweets_ct, na.rm=TRUE) # 5551

# Although a current metric of misinformation on the platform is not readily available, this percentage seems rather low given the typical user experience and general prevalence of this content, especially during the 2024 election. (no graph)


# How much "total engagement" does this represent?
# 
# Notes attached to Tweets representing 13% of all views
sum(df_tweets_noted_sum$twt_views, na.rm=TRUE) / sum(df_tweets_sum$twt_views, na.rm=TRUE)
# but smaller number actually published



# Average note per bad Post?  Top offenders?
stop





# hist of unlogged nt_publish_min
ggplot(df, aes(x = nt_publish_min/60, alpha=0.5)) +
  geom_histogram(bins = 50, color='black') +
  geom_vline(aes(xintercept = median(nt_publish_min/60)), color='green', linetype='dotted', size=1) +
  labs(title = "Histogram of nt_publish_min", x = "nt_publish_min", y = "Count")


# box and qq plots of nt_publish_min
ggplot(df, aes(x = nt_publish_min)) +
  geom_boxplot() +
  labs(title = "Boxplot of nt_publish_min", x = "nt_publish_min", y = "Count")

ggplot(df, aes(sample = nt_publish_min)) +
  geom_qq() +
  geom_qq_line() +
  labs(title = "QQ plot of nt_publish_min", x = "Theoretical Quantiles", y = "Sample Quantiles")


# mean and stats for nt_publish_min
df_notes_pub %>%
  summarise(mean = mean(nt_publish_min/60),
            median = median(nt_publish_min/60),
            sd = sd(nt_publish_min/60),
            min = min(nt_publish_min/60),
            max = max(nt_publish_min/60))

library(moments)
skewness(df$nt_publish_min) # 9.56
kurtosis(df$nt_publish_min) # 99.02

# quintiles
quantile(df_notes_pub$nt_publish_min/60, c(.25,.5,.75,1))

##
## Modeling
##

# data prep for modeling

df_mod <- df_notes_pub %>%
  select(-all_of(c('nt_status_current','nt_status_changed', 'nt_publish_revoked', 'nt_publish_revoked_min'))) %>%
  mutate(twt_lang = as.factor(twt_lang))

# imputation - twt_views
library(mice, warn.conflicts = FALSE)

imp <- mice(df_mod, maxit = 5, m = 5, 
  predictorMatrix = {
    pm <- make.predictorMatrix(df_mod)
    pm[,] <- 0
    pm["twt_views", c("twt_replies", "twt_retweets", "twt_likes", "twt_quotes","user_follower_ct","user_friend_ct")] <- 1
    pm
  })

stripplot(imp, twt_views, pch = 19, xlab = "Imputation number")
df_mod_imp <- complete(imp, action = 2)

## multicolliearity

# check for aliases (duplicate or linearly dependent columns)
df_alias <- alias(lm(nt_publish_min ~ ., data = df_mod_imp))
# remove 'nt_class_notmis_opinion','nr_rating_helpful_not','nr_mean_agg'

# check for high correlations
c_df_mod_imp <- correlate(df_mod_imp)
c_df_mod_imp %>% shave(upper=FALSE) %>% rplot()

cor_long <- c_df_mod_imp %>%
  pivot_longer(cols = -term, names_to = "variable", values_to = "correlation") %>%
  filter(!is.na(correlation), abs(correlation) >= 0.85) %>%
  filter(term != variable) %>%
  rowwise() %>%
  mutate(pair = paste(sort(c(term, variable)), collapse = "_")) %>%
  ungroup() %>%
  distinct(pair, .keep_all = TRUE) %>%
  select(term, variable, correlation)

cor_long
# remove polar_std, polar_entropy, ratings_agg, nr_nothelpful_missing, nr_nothelpful_nnn

# manual selection
df_mod_imp <- df_mod_imp %>%
  select(-any_of(c('nt_class_notmis_opinion','nr_rating_helpful_not','nr_mean_agg','nr_polar_std','nr_polar_entropy','nr_ratings_agg','nr_nothelpful_missing','nr_nothelpful_nnn')))

# discuss polar metrics in context of published vs unpublished


## Robust regression - Huber - M 
# MASS rlm()
# https://www.rdocumentation.org/packages/MASS/versions/7.3-65/topics/rlm

rr_recipe <- 
  recipe(nt_publish_min ~ ., data = df_mod_imp) %>%
  step_log(all_outcomes()) %>%
  step_normalize(all_numeric_predictors()) %>%
  step_dummy(twt_lang)

# inspect transformations and/or pass to other models
df_rr_prep <- prep(rr_recipe, training = df_mod_imp) %>% bake(new_data=NULL)

hr_mod = rlm(nt_publish_min ~ ., 
             data=df_rr_prep, 
             method=c("M"),
             wt.method = c("inv.var"))

hr_mod

# check resids
plot(hr_mod$residuals ~ fitted(hr_mod))
abline(h = 0, col = "red")

# same dataset, use robustbase to get stats
library(robustbase)

rr_mod2 <- lmrob(nt_publish_min ~ ., data=df_rr_prep)
summary(rr_mod2)

# step not available for rlm or lmrob, try lm to get features
library(MASS)
lm_step = stepAIC(lm(nt_publish_min ~ ., data=df_rr_prep), direction='both')
coef(lm_step)
selected_vars <- names(coef(lm_step))[-1]
selected_vars <- gsub("TRUE$", "", selected_vars)
selected_vars

formula <- as.formula(paste("nt_publish_min ~ ", paste(selected_vars, collapse = " + ")))
rr_mod3 <- lmrob(formula, data=df_rr_prep)
summary(rr_mod3)

stop

# still not great R^2

# https://www.spsanderson.com/steveondata/posts/2023-11-28/index.html
# https://stats.oarc.ucla.edu/r/dae/robust-regression/
# https://stats.stackexchange.com/questions/13702/what-is-the-difference-between-lm-and-rlm
# https://cran.r-project.org/web/packages/robustbase/index.html
# https://cran.r-project.org/web/packages/robustbase/robustbase.pdf




##
## Quantile Regression
##

library(quantreg)
# help(rq)

# Quantile Regression example
df_qr_test <- df_qr_prep %>%
  select(nt_publish_min, nr_helpful_address, twt_likes, nt_class_misinfo)

taus = c(0.05, 0.25, 0.5, 0.75, 0.95)
fit <- rq(nt_publish_min ~ ., tau = taus, data=df_qr_test)
summary(fit)
summary(fit, se = "boot") # bootstrapped CEs
summary(fit, se = "boot", R = 1000) # more replications
plot(summary(fit))

# Interpretation:
# 	•	Coefficients represent the marginal effect of predictors at specific quantiles of the response.
# 	•	E.g., if likes has a coefficient of 2 at τ = 0.75, then for each additional like, the 75th percentile of time_elapsed increases by ~2 units, holding other variables constant.
# 	•	Useful for detecting heterogeneity in predictor effects across the distribution, especially in presence of outliers or skewed distributions.

 # back-transform due to logged response and normalized predictors


# quantile regression
qr_recipe <- 
  recipe(nt_publish_min ~ ., data = df_mod_imp) %>%
  step_log(all_outcomes()) %>%
  step_normalize(all_numeric_predictors()) %>%
  step_dummy(twt_lang)

# df for qr
df_qr_prep <- prep(qr_recipe, training = df_mod_imp) %>% bake(new_data=NULL)

# fit qr on quartiles
qr_mod1 <- rq(nt_publish_min ~ ., tau = c(0.25, 0.5, 0.75, 1.0), data = df_qr_prep)

# bootstrap SEs
qr_results <- summary(qr_mod1, se = "boot", R = 1000)
#plot(summary(qr_mod1))

#qr_results[[1]]$coefficients

# function to back-transform based on log response
func_qr_back <- function(df){
  df %>%
    rownames_to_column() %>%
    mutate(`Std. Error` = exp(Value) * `Std. Error`, # goes first
            Value = ifelse(rowname == '(Intercept)', -exp(Value), exp(Value)-1), 
            `t value` = Value / `Std. Error`, 
            ci_lo = Value - 1.96 * `Std. Error`, 
            ci_hi = Value + 1.96 * `Std. Error`)}

# back-transform and join tables
df_qr_q25 <- as.data.frame(qr_results[[1]]$coefficients) %>%
  func_qr_back %>%
  rename(val_25 = 2, se_25 = 3, t_25 = 4, p_25 = 5, ci_lo_25 = 6, ci_hi_25 = 7)
  
df_qr_q50 <- as.data.frame(qr_results[[2]]$coefficients) %>% 
  func_qr_back %>%
  rename(val_50 = 2, se_50 = 3, t_50 = 4, p_50 = 5, ci_lo_50 = 6, ci_hi_50 = 7)

df_qr_q75 <- as.data.frame(qr_results[[3]]$coefficients) %>% 
  func_qr_back %>%
  rename(val_75 = 2, se_75 = 3, t_75 = 4, p_75 = 5, ci_lo_75 = 6, ci_hi_75 = 7)

df_qr_q100 <- as.data.frame(qr_results[[4]]$coefficients) %>% 
  func_qr_back %>%
  rename(val_100 = 2, se_100 = 3, t_100 = 4, p_100 = 5, ci_lo_100 = 6, ci_hi_100 = 7)

df_qr_results <- 
  Reduce(function(x, y) left_join(x, y, by = 'rowname'), list(df_qr_q25, df_qr_q50, df_qr_q75, df_qr_q100)) %>%
  mutate(sum_p = rowSums(select(., starts_with("p_")))) %>%
  left_join(df_nicenames)

# graphing

# scratch
df_qr_results_sample <- df_qr_results %>%
  filter(rowname == 'nt_is_medianoteTRUE') %>%
  select(any_of(starts_with(c('val_','ci_','p_')))) %>%
  pivot_longer(cols = everything(),
               names_to = c(".value", "quantile"),
               names_pattern = "(.*)_(\\d+)") %>%
  mutate(quantile = as.numeric(quantile),
    color = case_when(p <= 0.05 ~ "green", p > 0.05 & p <= 0.10 ~ "yellow", p > 0.10 & p <= 0.20 ~ "orange", TRUE ~ "red"))

df_qr_results_sample %>%
  ggplot(aes(x = quantile, y = val)) +
    geom_line() +
    geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.2) +
    geom_point(aes(color = color), size = 3) +
    geom_text(aes(label = round(val*100, 1)), vjust = -1.2, size = 3) +
    scale_color_identity() +
    scale_x_continuous(breaks = c(25, 50, 75, 100)) +
    labs(x = "Quantile", y = "Value", title = "Val by Quantile with Confidence Intervals") +
    theme_minimal()
  


# function for qr graphs
func_qr_graphs <- function(df, var)(
  df %>%
    filter(rowname == var) %>%
    select(any_of(starts_with(c('val_','ci_','p_')))) %>%
    pivot_longer(cols = everything(),
                names_to = c(".value", "quantile"),
                names_pattern = "(.*)_(\\d+)") %>%
    mutate(
      quantile = as.numeric(quantile),
      color = case_when(p <= 0.05 ~ "green", p > 0.05 & p <= 0.10 ~ "yellow", p > 0.10 & p <= 0.20 ~ "orange", TRUE ~ "red")) %>%
    ggplot(aes(x = quantile, y = val)) +
      geom_line() +
      geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.2) +
      geom_point(aes(color = color), size = 3) +
      geom_text(aes(label = paste0(round(val * 100, 0), "%")), vjust = -1.2, size = 3) +
      geom_hline(yintercept = 0, linetype = "dashed") +
      scale_color_identity() +
      scale_x_continuous(breaks = c(25, 50, 75, 100)) +
      labs(x = "Quantile", y = "Value", title = var) +
      theme_minimal()
)

# v2 with nicename
func_qr_graphs <- function(df, var) {
  df_var <- df %>% filter(rowname == var)
  nicename <- unique(df_var$nicename)

  df_var %>%
    select(any_of(starts_with(c('val_','ci_','p_')))) %>%
    pivot_longer(cols = everything(),
                 names_to = c(".value", "quantile"),
                 names_pattern = "(.*)_(\\d+)") %>%
    mutate(
      quantile = as.numeric(quantile),
      color = case_when(p <= 0.05 ~ "green",
                        p > 0.05 & p <= 0.10 ~ "yellow",
                        p > 0.10 & p <= 0.20 ~ "orange",
                        TRUE ~ "red")) %>%
    ggplot(aes(x = quantile, y = val)) +
      geom_line() +
      geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.2) +
      geom_point(aes(color = color), size = 3) +
      geom_text(aes(label = paste0(round(val * 100, 0), "%")), vjust = -1.2, size = 3) +
      geom_hline(yintercept = 0, linetype = "dashed") +
      scale_color_identity() +
      scale_x_continuous(breaks = c(25, 50, 75, 100)) +
      labs(x = "Quantile", y = "Value", title = nicename)
    }





# display in ascending order of total p-vals
for (row in df_qr_results %>% filter(rowname != '(Intercept)') %>%arrange(sum_p) %>% pull(rowname)) {
  print(func_qr_graphs(df_qr_results, row))
}

library(patchwork)

# top by sum_p
top_qr_rows <- df_qr_results %>%
  filter(rowname != "(Intercept)") %>%
  arrange(sum_p) %>%
  slice_head(n = 12) %>%
  pull(rowname)

plots <- lapply(top_qr_rows, function(row) func_qr_graphs(df_qr_results, row))
wrap_plots(plots, ncol = 3) 

# top by individual quantiles

# by p (use this)
top_qr_rows <- df_qr_results %>%
  filter(rowname != "(Intercept)") %>%
  arrange(p_100) %>%
  slice_head(n = 6) %>%
  pull(rowname)

# by abs(val)
top_qr_rows <- df_qr_results %>%
  filter(rowname != "(Intercept)") %>%
  arrange(desc(abs(val_100)), p_25) %>%
  slice_head(n = 12) %>%
  pull(rowname)

plots <- lapply(top_qr_rows, function(row) func_qr_graphs(df_qr_results, row))
wrap_plots(plots, ncol = 3, heights=5) 

top_names <- c('nr_ratings_ct','nr_nothelpful_incorrect','nt_is_medianoteTRUE','user_blueTRUE')

# by group by p (use this)
top_qr_rows <- df_qr_results %>%
  filter(str_starts(rowname, "user_")) %>%
  arrange(sum_p) %>%
  slice_head(n = 12) %>%
  pull(rowname)

plots <- lapply(top_qr_rows, function(row) func_qr_graphs(df_qr_results, row))
wrap_plots(plots, ncol = 3, heights=5) 


write.csv(df_qr_results$rowname, 'data/rownames.csv')
df_qr_results$rowname

stop


# http://www.econ.uiuc.edu/~roger/research/rq/vig.pdf
# https://drkebede.medium.com/quantile-regression-tutorial-in-r-f2eec72c132b
# https://library.virginia.edu/data/articles/getting-started-with-quantile-regression





##
## Non-Parametric Models
##

# Random Forest (bagged)
library(ranger)

rf_recipe <- 
  recipe(nt_publish_min ~ ., data = df_mod) %>%
  step_log(all_outcomes())

df_rf_prep <- prep(rf_recipe, training = df_mod) %>% bake(new_data=NULL)

rf_fit <- rand_forest(mode='regression', trees=1000) %>%
  set_engine('ranger', importance='impurity') %>%
  fit(nt_publish_min ~ ., df_rf_prep)

print(rf_fit)

rf_varimp <- data.frame(
  variable = names(rf_fit$fit$variable.importance),
  importance = round(rf_fit$fit$variable.importance,3)) %>%
  arrange(desc(importance))

ggplot(rf_varimp[1:10,], aes(x = reorder(variable, importance), y = importance)) +
  geom_col(fill='blue', alpha = 0.7) +
  coord_flip() +
  labs(title = "RF - Top 10 Variable Importances", x = NULL, y = "Importance")

# these results seem sensible
