-- create notes master table
create or replace table analysis.notes as 
with tbl as (
select
	n.* exclude (nt_class_believable, nt_class_harmful, nt_class_validation) -- old rating schema, ended 2022
	, ns.* exclude (nt_id, nt_author_id, nt_ts_created) -- dupes, have date and don't need timestamp
	, np.* exclude (nt_id) -- dupes
	, nr.* exclude (nt_id, nr_ratings_ct, nr_rating_helpful, nr_rating_helpful_somewhat, nr_rating_helpful_not)
	, t.* exclude (twt_id)
	, us.user_follower_ct
	, us.user_friend_ct
	, us.user_blue::BOOLEAN as user_blue -- fix earlier
	, us.user_created_dt
	, datediff('day', us.user_created_dt, t.twt_dt_created) as user_account_age
	from main.notes_base n
	left join main.note_status_base ns
		on n.nt_id = ns.nt_id
	left join main.note_ratings_polar np
		on n.nt_id = np.nt_id
	left join main.note_ratings_agg_pct nr
		on n.nt_id = nr.nt_id
	left join main.tweets_base t
		on n.twt_id = t.twt_id
	left join main.tweets_users_twt_daily us
		on us.user_id = t.twt_author_id
		and us.twt_dt_created = t.twt_dt_created
)
select * from tbl
where nt_status_current is not null;
-- timing: 4m



-- all notes with a current status
select count(*) from analysis.notes; -- 1,648,592
-- dropped notes with no status info - missing source data


-- all notes with a corresponding tweet from the elections dataset
create or replace table analysis.notes_election as
select 
	* 
	, datediff('min', twt_ts_created, nt_ts_created) nt_created_min
	, case 
		when nt_status_current = 'CURRENTLY_RATED_HELPFUL' then 1
		when nt_status_current = 'NEEDS_MORE_RATINGS' and nt_status_nonnmr_first = 'CURRENTLY_RATED_HELPFUL' then 1
		else 0
		end as nt_publish
	, case 
		when nt_status_current = 'CURRENTLY_RATED_HELPFUL' then datediff('min', nt_ts_created, nt_ts_nonnmr_first)
		when nt_status_current = 'NEEDS_MORE_RATINGS' and nt_status_nonnmr_first = 'CURRENTLY_RATED_HELPFUL' then datediff('min', nt_ts_created, nt_ts_nonnmr_first)
		else null 
	  	end as nt_publish_min
	, case 
		when nt_status_current = 'NEEDS_MORE_RATINGS' and nt_status_nonnmr_first = 'CURRENTLY_RATED_HELPFUL' then 1 else 0 
		end as nt_publish_revoked
	, case 
		when nt_status_current = 'NEEDS_MORE_RATINGS' and nt_status_nonnmr_first = 'CURRENTLY_RATED_HELPFUL' then datediff('min', nt_ts_created, nt_ts_status_current)
		else null
		end as nt_publish_revoked_min
	, case 
		when nt_status_current = 'NEEDS_MORE_RATINGS' and nt_status_nonnmr_first = 'CURRENTLY_RATED_HELPFUL' then 1
		when nt_status_current = 'NEEDS_MORE_RATINGS' and nt_status_nonnmr_first = 'CURRENTLY_RATED_NOT_HELPFUL' then 1
		else 0
		end as nt_status_changed
from analysis.notes
where twt_dt_created >= '2023-12-31';
-- timing: 20s


-- counts
select count(*) from analysis.notes_election;  -- 13,745     
select count(distinct twt_id) from analysis.notes_election; -- 5,551
select count(distinct twt_id) from main.tweets_base; -- 38,497,288
select count(distinct twt_author_id) from analysis.notes_election; -- 2264


-- blues
with users as (select twt_author_id, user_blue, row_number() over (partition by twt_author_id order by user_blue desc) as rn from analysis.notes_election),
uniq as (select * exclude (rn) from users where rn = 1)
select user_blue, count(*) from uniq group by 1

--False	564
--True	1700


-- class breakdowns
select
	nt_status_current
	, nt_status_nonnmr_recent
	, nt_status_nonnmr_first
	, count(*) ct
from analysis.notes_election
group by 1,2,3
order by 1,2,3;


--CURRENTLY_RATED_HELPFUL	CURRENTLY_RATED_HELPFUL	CURRENTLY_RATED_HELPFUL	361
--CURRENTLY_RATED_NOT_HELPFUL	CURRENTLY_RATED_NOT_HELPFUL	CURRENTLY_RATED_NOT_HELPFUL	593
--NEEDS_MORE_RATINGS	CURRENTLY_RATED_HELPFUL	CURRENTLY_RATED_HELPFUL	224
--NEEDS_MORE_RATINGS	CURRENTLY_RATED_NOT_HELPFUL	CURRENTLY_RATED_NOT_HELPFUL	329
--NEEDS_MORE_RATINGS			12238



