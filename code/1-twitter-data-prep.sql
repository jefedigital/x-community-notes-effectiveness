-- schema 'main' - raw and base tables
-- schema 'analysis' - secondary tables
create schema analysis;

--
-- tweets
--

-- load data
--drop table if exists main.tweets_raw;
--create or replace table main.tweets_raw  as select * from read_parquet('/Volumes/seagate_5tb/data/usc-x-24-us-election-parquet/*/*.parquet', union_by_name=True);


-- clean up tweets_raw
create or replace table main.tweets_clean as select
	id::VARCHAR twt_id
	,strptime(date, '%Y-%m-%d') twt_dt_created
	,to_timestamp(epoch) twt_ts_created
	,regexp_extract(user, '(''id_str'': )''([^'']*)''' ,2)::VARCHAR twt_author_id
	,conversationId::VARCHAR twt_convo_id
	,lang twt_lang
	,text twt_text
	,url twt_url
	,json_extract(replace(trim(hashtags, '"'), '''', '"')::JSON, '$[*].text') twt_hashtags
	,case
		when contains(viewCount, '{') THEN try_cast(regexp_extract(regexp_replace(viewCount, '''|"', '', 'g'), '(count: )(.*),' ,2) as INT)
		else viewCount::INT
		end as twt_views
	,replyCount::INT twt_replies
	,retweetCount::INT twt_retweets
	,likeCount::INT twt_likes
	,quoteCount::INT twt_quotes
	,retweetedTweet twt_is_retweet
	,retweetedTweetID::VARCHAR twt_retweet_id
	,retweetedUserID::VARCHAR twt_retweet_author_id
	,quotedTweet::BOOLEAN twt_is_quote
	,in_reply_to_status_id_str::VARCHAR twt_reply_id
	,in_reply_to_user_id_str::VARCHAR twt_reply_author_id
	,regexp_extract_all(mentionedUsers, '(''id_str'': )''([^'']*)''' ,2) twt_mentioned_author_ids
	,regexp_extract_all(replace(links, '\"', ''''), '(''expanded_url'': )''([^'']*)''' ,2) as twt_links
	,regexp_extract_all(media, '(''type'': )''([^'']*)''' ,2) twt_media_type
	,regexp_extract_all(media, '(''media_url_https'': )''([^'']*)''' ,2) twt_media_img
	,regexp_extract_all(media, '(''content_type'':.*''url'': )''([^'']*)''' ,2) twt_media_vid
from main.tweets_raw;


-- dedupe tweets from orig data 
-- (memory issues, have to do in stages)

-- create agg_metrics to rank dupes with highest counts (i.e. latest)
create table main.tweets_base_dedupe_1 as 
select *, twt_views + twt_replies + ifnull(twt_retweets,0) + twt_likes + twt_quotes as agg_metrics 
from main.tweets_clean;

-- tweets that are duped
create table main.tweets_base_dedupe_2 as (
with id_counts as (select twt_id, count(*) ct from tweets_base_dedupe_1 group by 1)
select twt_id from id_counts where ct > 1
);

-- rank duped tweets by agg_metric
create table main.tweets_base_dedupe_3 as
select row_number() over (partition by twt_id order by agg_metrics desc) as rn, * 
from main.tweets_base_dedupe_1
where twt_id in (select twt_id from main.tweets_base_dedupe_2);


-- dedupe
create table main.tweets_base_dedupe_4 as
select * exclude (rn, agg_metrics)
from main.tweets_base_dedupe_3
where rn = 1;

select count(*) from main.tweets_base_dedupe_4; -- 989354

-- table of non-duped tweets
create table main.tweets_base_dedupe_5 as
select * from main.tweets_clean
where twt_id not in (select twt_id from main.tweets_base_dedupe_2);

select count(*) from main.tweets_base_dedupe_5; -- 37643421


-- final

create table main.tweets_base_dedupe as
select * from main.tweets_base_dedupe_4
union
select * from main.tweets_base_dedupe_5;


-- dump to parquet and reload
copy (select * from main.tweets_base_dedupe_4) to '/Volumes/seagate_5tb/data/tweets_deduped.parquet';
copy (select * from main.tweets_base_dedupe_5) to '/Volumes/seagate_5tb/data/tweets_nodupes.parquet';

create table main.tweets_base_dedupe as select * from read_parquet('/Volumes/seagate_5tb/data/fixes-tweets/*.parquet', union_by_name=True);


-- qa
select count(*) from main.tweets_base; -- 40022630
select count(*) from main.tweets_base_dedupe; -- 38632775

with tb1 as (select twt_id, count(*) ct from main.tweets_base_dedupe group by 1)
select * from tb1 where ct > 1 order by 2 desc; -- no dupes


-- final tweets base table, apply date range filter and misc corrections
create or replace table main.tweets_base as
select 
	dd.* exclude (twt_is_retweet)
	,dd.twt_is_retweet::BOOLEAN as twt_is_retweet
from main.tweets_base_dedupe dd
where twt_dt_created >= '2023-12-31';

select twt_is_retweet, count(*) from tweets_base group by 1;



--
-- twitter accounts
--


-- from tweet author data
-- we want one row per user per day, if a user tweeted on that day

create or replace table tweets_users_twt_daily as
with tbl as (
select
	strptime(date, '%Y-%m-%d') twt_dt_created
	,to_timestamp(epoch) twt_ts_created
	,regexp_extract(user, '(''id_str'': )''([^'']*)''' ,2)::VARCHAR user_id
	,regexp_extract(url, '(^https:\/\/twitter\.com\/)(.*?)\/',2) user_name
	,regexp_extract(user, '(''username'': )(.*?),',2)::VARCHAR user_screen_name
	,regexp_extract(user, '(''rawDescription'': )''([^'']*)''',2)::VARCHAR user_desc
	,regexp_extract(user, '(''followersCount'': )(.*?),',2)::VARCHAR user_follower_ct
	,regexp_extract(user, '(''friendsCount'': )(.*?),',2)::VARCHAR user_friend_ct
	,regexp_extract(user, '(''favouritesCount'': )(.*?),',2)::VARCHAR user_favorites_ct
	,regexp_extract(user, '(''statusesCount'': )(.*?),',2)::VARCHAR user_status_ct
	,regexp_extract(user, '(''blue'': )(.*?),',2)::VARCHAR::BOOLEAN user_blue
	,replace(trim(substring(regexp_extract(user, '(''created'': )(.*?)\ tz',2),19,12), ', '), ', ', '-')::DATE user_created_dt
from tweets_raw
),
rows as (
select
	row_number() over (partition by user_id, twt_dt_created order by twt_ts_created desc) as rn
	,*
from tbl
)
select * exclude (rn) from rows where rn = 1;
-- timing: 20m



-- qa and eda
select * from tweets_users_twt_daily limit 10;
select user_id, count(*) from tweets_users_twt_daily group by 1 order by 2 desc;  -- some prolific tweeters


-- deduped user list on most recent user info
create or replace table tweets_users_uniq as
with users as (
	select
		row_number() over (partition by user_id order by twt_dt_created desc) rn
		,*
	from tweets_users_twt_daily 
)
select * exclude (rn)
from users 
where rn = 1;
-- timing: 2m


select * from tweets_users_uniq limit 10;
select user_id, count(*) from tweets_users_uniq group by 1 order by 2 desc, 1 limit 10; -- deduped
select count(*) from tweets_users_uniq; -- 4,210,447


-- how many user_blue accounts?
select user_blue, count(*) from tweets_users_uniq group by 1; -- 11.6% overall

--True	438842
--False	3771605



-- replied-to authors, deduped and clean
create or replace table tweets_users_rply as
with users as (
	select
		in_reply_to_user_id_str::BIGINT::VARCHAR as user_id
		,in_reply_to_screen_name as user_screen_name
		,to_timestamp(epoch) twt_ts_created
		, row_number() over (partition by in_reply_to_user_id_str order by epoch desc) rn
	from main.tweets_raw
	where in_reply_to_user_id_str is not null
)
select
	user_id
	, user_screen_name
from users 
where rn = 1
	and right(user_id, 7) != '0000000';
-- timing: 40s


select * from tweets_users_rply limit 10; 


-- from mentioned authors
create or replace table tweets_users_mtn as
select distinct
	mentionedUsers -- array
from main.tweets_raw
where mentionedUsers is not null;
-- timing: 6m

select * from tweets_users_mtn limit 10;
-- having trouble unnesting these -- skipping for now


-- join tables
create or replace table tweets_users_base as
with users as (
select
	user_id
	,user_name
	,user_screen_name
from tweets_users_uniq
)
select distinct
	coalesce(users.user_id, ur.user_id) user_id
	,users.user_name user_name
	,coalesce(users.user_screen_name, ur.user_screen_name) user_screen_name
from users
full join tweets_users_rply as ur
	on users.user_id = ur.user_id;
-- timing: 13s


-- still dupes?
select user_id, count(*) from main.tweets_users_base group by 1 order by 2 desc, 1;


--
-- community notes
--

-- load data

--drop table if exists note_status_raw;
--create table main.note_status_raw as select * from read_parquet('/Volumes/seagate_5tb/data/x-community-notes-parquet/note_status_history/*.parquet', union_by_name=True);
--
--drop table if exists notes_raw;
--create table main.notes_raw as select * from read_parquet('/Volumes/seagate_5tb/data/x-community-notes-parquet/notes/*.parquet', union_by_name=True);
--
--drop table if exists note_ratings_raw;
--create table main.note_ratings_raw as select * from read_parquet('/Volumes/seagate_5tb/data/x-community-notes-parquet/ratings/*.parquet', union_by_name=True);
--
--drop table if exists note_users_raw;
--create table main.note_users_raw as select * from read_parquet('/Volumes/seagate_5tb/data/x-community-notes-parquet/user_enrollment/*.parquet', union_by_name=True);



-- notes_base
-- data dict https://communitynotes.x.com/guide/en/under-the-hood/download-data

create or replace table main.notes_base as
select
	noteId::VARCHAR nt_id
	, noteAuthorParticipantId nt_author_id
	, to_timestamp(round(createdAtMillis/1000,0)) nt_ts_created
	, tweetId::VARCHAR twt_id
	, isMediaNote::BOOLEAN nt_is_medianote
	, summary nt_summary
	, case when classification = 'NOT_MISLEADING' then False else True end nt_class_misinfo
	, believable nt_class_believable
	, harmful nt_class_harmful
	, trustworthySources::BOOLEAN nt_class_trustsource
	, validationDifficulty nt_class_validation
	, misleadingOther::BOOLEAN nt_class_mis_misleading
	, misleadingFactualError::BOOLEAN nt_class_mis_notfactual
	, misleadingManipulatedMedia::BOOLEAN nt_class_mis_mediamanip
	, misleadingOutdatedInformation::BOOLEAN nt_class_mis_outdated
	, misleadingMissingImportantContext::BOOLEAN nt_class_mis_nocontext
	, misleadingUnverifiedClaimAsFact::BOOLEAN nt_class_mis_unverified
	, misleadingSatire::BOOLEAN nt_class_mis_satire
	, notMisleadingOther::BOOLEAN nt_class_notmis_other
	, notMisleadingFactuallyCorrect::BOOLEAN nt_class_notmis_correct
	, notMisleadingOutdatedButNotWhenWritten::BOOLEAN nt_class_notmis_notoutdated
	, notMisleadingClearlySatire::BOOLEAN nt_class_notmis_satire
	, notMisleadingPersonalOpinion::BOOLEAN nt_class_notmis_opinion
from main.notes_raw;
-- timing: 36s



-- note_ratings_base

-- drop version = 1, switched over from v1 to v2 between Jun 9-30 2021.
-- select min(to_timestamp(createdatMillis/1000)), max(to_timestamp(createdatMillis/1000)), version from note_ratings_raw group by 3;
-- standardize the "helpfulness" scores in separate columns and include user, timestamp, agree/disagree. 

create or replace table main.note_ratings_base as
SELECT 
    noteId nt_id
    , raterParticipantId nr_author_id
    , to_timestamp(round(createdatMillis/1000,0)) as nr_ts_rated
    , (CASE WHEN helpfulnessLevel = 'HELPFUL' THEN 1 ELSE 0 end as nr_rating_helpful
    , CASE WHEN helpfulnessLevel = 'SOMEWHAT_HELPFUL' THEN 1 ELSE 0 end as nr_rating_helpful_somewhat
    , CASE WHEN helpfulnessLevel = 'NOT_HELPFUL' THEN 1 ELSE 0 end as nr_rating_helpful_not
    , helpfulOther::INT1 nr_helpful_other
    , helpfulInformative::INT1 nr_helpful_info
    , helpfulClear::INT1 nr_helpful_clear
    , helpfulEmpathetic::INT1 nr_helpful_empathy
    , helpfulGoodSources::INT1 nr_helpful_sources
    , helpfulUniqueContext::INT1 nr_helpful_ctxt_unique
    , helpfulAddressesClaim::INT1 nr_helpful_address
    , helpfulImportantContext::INT1 nr_helpful_ctxt_impt
    , helpfulUnbiasedLanguage::INT1 nr_helpful_unbiased
    , notHelpfulOther::INT1 nr_nothelpful_other
    , notHelpfulIncorrect::INT1 nr_nothelpful_incorrect
    , notHelpfulSourcesMissingOrUnreliable::INT1 nr_nothelpful_source
    , notHelpfulOpinionSpeculationOrBias::INT1 nr_nothelpful_opinion_bias
    , notHelpfulMissingKeyPoints::INT1 nr_nothelpful_missing
    , notHelpfulOutdated::INT1 nr_nothelpful_outdated
    , notHelpfulHardToUnderstand::INT1 nr_nothelpful_understand
    , notHelpfulArgumentativeOrBiased::INT1 nr_nothelpful_argument
    , notHelpfulOffTopic::INT1 nr_nothelpful_offtopic
    , notHelpfulSpamHarassmentOrAbuse::INT1 nr_nothelpful_spam
    , notHelpfulIrrelevantSources::INT1 nr_nothelpful_source_irrev
    , notHelpfulOpinionSpeculation::INT1 nr_nothelpful_opinion_spec
    , notHelpfulNoteNotNeeded::INT1 nr_nothelpful_nnn
FROM main.note_ratings_raw
where version = 2;
-- timing: 2m 24s


-- note_ratings_agg
-- aggregated by note id -- huge query, takes time

create or replace table main.note_ratings_agg as
SELECT 
	nt_id
	, count() as nr_ratings_ct
	, sum(nr_rating_helpful) nr_rating_helpful
	, sum(nr_rating_helpful_somewhat) nr_rating_helpful_somewhat
	, sum(nr_rating_helpful_not) nr_rating_helpful_not
	, sum(nr_helpful_other) nr_helpful_other
	, sum(nr_helpful_info) nr_helpful_info
	, sum(nr_helpful_clear) nr_helpful_clear
	, sum(nr_helpful_empathy) nr_helpful_empathy
	, sum(nr_helpful_sources) nr_helpful_sources
	, sum(nr_helpful_ctxt_unique) nr_helpful_ctxt_unique
	, sum(nr_helpful_address) nr_helpful_address
	, sum(nr_helpful_ctxt_impt) nr_helpful_ctxt_impt
	, sum(nr_helpful_unbiased) nr_helpful_unbiased
	, sum(nr_nothelpful_other) nr_nothelpful_other
	, sum(nr_nothelpful_incorrect) nr_nothelpful_incorrect
	, sum(nr_nothelpful_source) nr_nothelpful_source
	, sum(nr_nothelpful_opinion_bias) nr_nothelpful_opinion_bias
	, sum(nr_nothelpful_missing) nr_nothelpful_missing
	, sum(nr_nothelpful_outdated) nr_nothelpful_outdated
	, sum(nr_nothelpful_understand) nr_nothelpful_understand
	, sum(nr_nothelpful_argument) nr_nothelpful_argument
	, sum(nr_nothelpful_offtopic) nr_nothelpful_offtopic
	, sum(nr_nothelpful_spam) nr_nothelpful_spam
	, sum(nr_nothelpful_source_irrev) nr_nothelpful_source_irrev
	, sum(nr_nothelpful_opinion_spec) nr_nothelpful_opinion_spec
	, sum(nr_nothelpful_nnn) nr_nothelpful_nnn
from main.note_ratings_base
group by 1;
-- timing: 31m 26s



-- note_ratings_agg_pct
-- proportions instead of hard counts
create or replace table main.note_ratings_agg_pct as
SELECT 
	nt_id
	, nr_ratings_ct
	, nr_rating_helpful / nr_ratings_ct as nr_rating_helpful
	, nr_rating_helpful_somewhat / nr_ratings_ct as nr_rating_helpful_somewhat
	, nr_rating_helpful_not / nr_ratings_ct as nr_rating_helpful_not
	, nr_helpful_other / nr_ratings_ct as nr_helpful_other
	, nr_helpful_info / nr_ratings_ct asnr_helpful_info
	, nr_helpful_clear / nr_ratings_ct as nr_helpful_clear
	, nr_helpful_empathy / nr_ratings_ct as nr_helpful_empathy
	, nr_helpful_sources / nr_ratings_ct as nr_helpful_sources
	, nr_helpful_ctxt_unique / nr_ratings_ct as nr_helpful_ctxt_unique
	, nr_helpful_address / nr_ratings_ct as nr_helpful_address
	, nr_helpful_ctxt_impt / nr_ratings_ct as nr_helpful_ctxt_impt
	, nr_helpful_unbiased / nr_ratings_ct as nr_helpful_unbiased
	, nr_nothelpful_other / nr_ratings_ct as nr_nothelpful_other
	, nr_nothelpful_incorrect / nr_ratings_ct as nr_nothelpful_incorrect
	, nr_nothelpful_source / nr_ratings_ct as nr_nothelpful_source
	, nr_nothelpful_opinion_bias / nr_ratings_ct as nr_nothelpful_opinion_bias
	, nr_nothelpful_missing / nr_ratings_ct as nr_nothelpful_missing
	, nr_nothelpful_outdated / nr_ratings_ct as nr_nothelpful_outdated
	, nr_nothelpful_understand / nr_ratings_ct as nr_nothelpful_understand
	, nr_nothelpful_argument / nr_ratings_ct as nr_nothelpful_argument
	, nr_nothelpful_offtopic / nr_ratings_ct as nr_nothelpful_offtopic
	, nr_nothelpful_spam / nr_ratings_ct as nr_nothelpful_spam
	, nr_nothelpful_source_irrev / nr_ratings_ct as nr_nothelpful_source_irrev
	, nr_nothelpful_opinion_spec / nr_ratings_ct as nr_nothelpful_opinion_spec
	, nr_nothelpful_nnn / nr_ratings_ct as nr_nothelpful_nnn
from main.note_ratings_agg;
-- timing: 16s


--
-- custom polarization metrics
--

-- note_ratings_polar
-- apply the standard and entropy polarization metrics, aggregated by note id
create or replace table main.note_ratings_polar as ( 
	with aggs as (
		select
			nt_id
			, nr_ratings_ct
			, nr_rating_helpful
			, nr_rating_helpful_somewhat
			, nr_rating_helpful_not
		from main.note_ratings_agg_pct
	),
	ratings as (
		select 
			*
			,((nr_rating_helpful * 1) + (nr_rating_helpful_somewhat* 0.5) + (nr_rating_helpful_not * -1)) * nr_ratings_ct as nr_ratings_agg
			,(nr_rating_helpful * 1) + (nr_rating_helpful_somewhat* 0.5) + (nr_rating_helpful_not * -1) as nr_mean_agg
		from aggs		
	)
	select
		*
		, (SQRT((nr_rating_helpful * (1-ABS(nr_mean_agg))^2) + (nr_rating_helpful_somewhat * (1-ABS(nr_mean_agg))^2) + (nr_rating_helpful_not * (1-ABS(nr_mean_agg))^2)))/2 as nr_polar_std -- corrected for probs
		, abs((nr_rating_helpful * if(nr_rating_helpful = 0, 0, log(nr_rating_helpful)/log(2))) + (nr_rating_helpful_somewhat * if(nr_rating_helpful_somewhat = 0, 0, log(nr_rating_helpful_somewhat)/log(2))) + (nr_rating_helpful_not * if(nr_rating_helpful_not = 0, 0, log(nr_rating_helpful_not)/log(2)))) as nr_polar_entropy -- corrected for probs
	from ratings 
	);


-- note_status_base
create or replace table main.note_status_base as
select
	noteId nt_id
	, noteAuthorParticipantId nt_author_id
	, lockedStatus nt_status_locked
	, currentStatus nt_status_current 
	, mostRecentNonNMRStatus nt_status_nonnmr_recent
	, firstNonNMRStatus nt_status_nonnmr_first
	, currentGroupStatus nt_status_group_current
	, currentCoreStatus nt_status_core_current
	, currentExpansionStatus nt_status_expansion_current
	, currentMultiGroupStatus nt_status_multi_current
	, currentDecidedBy nt_status_decided_current
	, currentModelingGroup nt_status_modelgroup_current
	, currentModelingMultiGroup nt_status_modelgroup_multi_current
	, to_timestamp(round(createdatMillis/1000,0)) nt_ts_created
	, to_timestamp(timestampMinuteOfFinalScoringOutput*60) nt_ts_score_final
	, to_timestamp(round(timestampMillisOfStatusLock/1000,0)) nt_ts_status_lock
	, to_timestamp(round(timestampMillisOfRetroLock/1000,0)) nt_ts_retro_lock
	, to_timestamp(round(timestampMillisOfCurrentStatus/1000,0)) nt_ts_status_current
	, to_timestamp(round(timestampMillisOfMostRecentStatusChange/1000,0)) nt_ts_status_recent
	, to_timestamp(round(timestampMillisOfLatestNonNMRStatus/1000,0)) nt_ts_nonnmr_recent
	, to_timestamp(round(timestampMillisOfFirstNonNMRStatus/1000,0)) nt_ts_nonnmr_first
	, to_timestamp(round(timestampMillisOfNmrDueToMinStableCrhTime/1000,0)) nt_ts_nmr_min_stable
	, to_timestamp(round(timestampMillisOfFirstNmrDueToMinStableCrhTime/1000,0)) nt_ts_nmr_min_stable_first
from main.note_status_raw;
-- timing: 17s


-- note_users_base
create or replace table main.note_users_base as
select
	participantId nu_id
	, enrollmentState nu_status
	, successfulRatingNeededToEarnIn nu_earnin_score_req
	, to_timestamp(round(timestampOfLastStateChange/1000,0)) nu_ts_status_change_last
	, case 
		when timestampOfLastEarnOut == 1 then null
		else to_timestamp(round(timestampOfLastEarnOut/1000,0)) 
	end nu_ts_earnout_last
	, modelingPopulation nu_model_population
	, modelingGroup nu_modelgroup
	, numberOfTimesEarnedOut nu_earnouts
from main.note_users_raw;

