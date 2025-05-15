
-- table for time-to-publication analysis
create or replace table analysis.notes_ttp as
select
	nt_id
	, nt_author_id
	, nt_ts_created
	, nt_is_medianote
	, nt_created_min
	, nt_status_current
	, nt_status_changed
	, nt_summary
	, COLUMNS('^nt_publish.*$')
	, COLUMNS('^nt_class.*$')
	, COLUMNS('^nr_.*$')
	, twt_id
	, twt_url
	, twt_author_id
	, twt_reply_author_id
	, twt_convo_id
	, twt_lang
	, twt_views
	, twt_replies
	, twt_retweets
	, twt_likes
	, twt_quotes
	, twt_is_retweet
	, twt_is_quote
	, user_follower_ct::INT as user_follower_ct
	, user_friend_ct::INT as user_friend_ct
	, user_blue
	, user_created_dt
	, user_account_age
from analysis.notes_election;
-- timing: 10s



-- most-noted tweets with publish history
select
	twt_id
	, twt_url
--	, twt_views
	, count(*) as n_notes
FROM analysis.notes_ttp
where nt_publish = 1
group by 1,2
order by n_notes desc
limit 10;


-- most-viewed noted tweets
select
	nt_id
	, nt_summary
	, nr_ratings_ct
	, nt_status_current
	, nt_publish_min/60 nt_publish_hr
	, nt_publish_revoked
	, twt_id
	, twt_url
	, twt_views
FROM analysis.notes_ttp
where nt_publish = 1
order by twt_views desc, twt_id
limit 30;



-- notes tweets with high TTPs and classified misinfo
select
	nt_id
	, round(nt_publish_min/60,1) nt_publish_hr
	, nt_status_current
	, nt_publish_revoked
	, nt_class_misinfo
	, nt_summary
	, nr_ratings_ct
	, round(nr_polar_std,2) nr_polar_std
	, twt_id
	, twt_url
	, twt_views
FROM analysis.notes_ttp
where nt_publish = 1 and nt_class_misinfo is True
order by nr_polar_std desc, twt_views desc
limit 1000;




-- top notes by overall tweet views
SELECT
	*
FROM analysis.notes_ttp
ORDER BY twt_views DESC
LIMIT 20;


-- corr of polar x mean_agg ?
select
	round(nr_polar_std,1)
	, round(nr_mean_agg,1)
	, count(*)
from analysis.notes_ttp
group by 1,2
order by 1,2
limit 100;


-- corr of polar x ratings_ct
select
	round(nr_polar_std,1)
	, round(nr_ratings_ct,-2)
	, count(*)
from analysis.notes_ttp
where nt_publish = 1
group by 1,2
order by 1 desc, 2 desc;
-- seems better after fix for probabilities


-- count of notes and mean ttp by polarity
select
	nt_class_misinfo
	, round(nr_polar_std,1) polar
	, median(nt_publish_min/60) median_hrs
	, count(*) n
from analysis.notes_ttp
group by 1,2
order by 2 desc, 1 desc;


-- 576 notes published that tagged misinfo 
select
	*
from analysis.notes_ttp
where nt_class_misinfo = True
	and nt_publish = 1; 


select
	count(*)
from analysis.notes_ttp
where nt_class_misinfo = True
	and nt_publish = 1; 

-- out of 8180
select
	count(*)
from analysis.notes_ttp
where nt_class_misinfo = True;




-- summary table of all tweets for EDA
-- filter for 2024, mark where twt_views are NA for imputing

create or replace table analysis.tweets_all_summary as
select
	twt_dt_created
	, count(*) as tweets_ct
	, sum(twt_views) twt_views
	, sum(twt_replies) twt_replies
	, sum(twt_retweets) twt_retweets
	, sum(twt_likes) twt_likes
	, sum(twt_quotes) twt_quotes
from main.tweets_base
where twt_dt_created > '2023-12-31'
group by 1;
-- timing: 12s



-- all NOTED tweets for EDA
create or replace table analysis.tweets_noted as 
with tweets as ( 
select * from main.tweets_base
where twt_id in (select twt_id from analysis.notes_election)
)
select 
	t.*
	, coalesce(u.user_name, u.user_screen_name) as twt_author_name
from tweets t
left join main.tweets_users_base u
	on t.twt_author_id = u.user_id;
-- timing: 2m



-- summary table of all NOTED tweets for EDA
create or replace table analysis.tweets_noted_summary as
select
	twt_dt_created
	, count(*) as tweets_ct
	, sum(twt_views) twt_views
	, sum(twt_replies) twt_replies
	, sum(twt_retweets) twt_retweets
	, sum(twt_likes) twt_likes
	, sum(twt_quotes) twt_quotes
from analysis.tweets_noted
group by 1;
	
