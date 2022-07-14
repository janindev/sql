select
	unif.* except(active_status),
	safe_multiply(unif.num_studios, unif.open_duration) as total_open_duration,
    
    case
    when unif.blocker_duration = 0 then 0
    when unif.open_duration = 0 then 0
    when (unif.blocker_duration / safe_multiply(unif.num_studios, unif.open_duration)) > 0.5 then 1
    else end 
    as share_blocked_weighed_50_flag

from

	(select distinct
		ent.shop_id,
		sh.district_lead,
		sh.region_lead,
		ent.entry_date,
		ent.entry_weekday,

		case
		when ent.entry_weekday = 'Monday' then sh.monday_open_duration
		when ent.entry_weekday = 'Tuesday' then sh.tuesday_open_duration
		when ent.entry_weekday = 'Wednesday' then sh.wednesday_open_duration
		when ent.entry_weekday = 'Thursday' then sh.thursday_open_duration
		when ent.entry_weekday = 'Friday' then sh.friday_open_duration
		when ent.entry_weekday = 'Saturday' then sh.saturday_open_duration end
		as open_duration,

		ent.blocker_duration,
		ent.customer_service_duration,
		ent.num_studios_in_use,
		ent.num_studios,
		sh.active_status,

	from

		(select
			ent1.*,

			case
			when extract(dayofweek from ent1.entry_date) = 2 then 'Monday'
			when extract(dayofweek from ent1.entry_date) = 3 then 'Tuesday'
			when extract(dayofweek from ent1.entry_date) = 4 then 'Wednesday'
			when extract(dayofweek from ent1.entry_date) = 5 then 'Thursday'
			when extract(dayofweek from ent1.entry_date) = 6 then 'Friday'
			when extract(dayofweek from ent1.entry_date) = 7 then 'Saturday'
			when extract(dayofweek from ent1.entry_date) = 1 then 'Sunday' end
			as entry_weekday

		from `sonova-marketing`.data.fact_shop_calendar_entry_summary ent1

		) ent


		left join

		(select
			sh1.shop_id,
			sh1.district_lead,
			sh1.region_lead,

			safe_add(
			case when time_diff(sh1.monday_open_time_1_end, sh1.monday_open_time_1_start, minute) is null then 0 else time_diff(sh1.monday_open_time_1_end, sh1.monday_open_time_1_start, minute) end,
			case when time_diff(sh1.monday_open_time_2_end, sh1.monday_open_time_2_start, minute) is null then 0 else time_diff(sh1.monday_open_time_2_end, sh1.monday_open_time_2_start, minute) end)
			as monday_open_duration,

			safe_add(
			case when time_diff(sh1.tuesday_open_time_1_end, sh1.tuesday_open_time_1_start, minute) is null then 0 else time_diff(sh1.tuesday_open_time_1_end, sh1.tuesday_open_time_1_start, minute) end,
			case when time_diff(sh1.tuesday_open_time_2_end, sh1.tuesday_open_time_2_start, minute) is null then 0 else time_diff(sh1.tuesday_open_time_2_end, sh1.tuesday_open_time_2_start, minute) end)
			as tuesday_open_duration,

			safe_add(
			case when time_diff(sh1.wednesday_open_time_1_end, sh1.wednesday_open_time_1_start, minute) is null then 0 else time_diff(sh1.wednesday_open_time_1_end, sh1.wednesday_open_time_1_start, minute) end,
			case when time_diff(sh1.wednesday_open_time_2_end, sh1.wednesday_open_time_2_start, minute) is null then 0 else time_diff(sh1.wednesday_open_time_2_end, sh1.wednesday_open_time_2_start, minute) end)
			as wednesday_open_duration,

			safe_add(
			case when time_diff(sh1.thursday_open_time_1_end, sh1.thursday_open_time_1_start, minute) is null then 0 else time_diff(sh1.thursday_open_time_1_end, sh1.thursday_open_time_1_start, minute) end,
			case when time_diff(sh1.thursday_open_time_2_end, sh1.thursday_open_time_2_start, minute) is null then 0 else time_diff(sh1.thursday_open_time_2_end, sh1.thursday_open_time_2_start, minute) end)
			as thursday_open_duration,

			safe_add(
			case when time_diff(sh1.friday_open_time_1_end, sh1.friday_open_time_1_start, minute) is null then 0 else time_diff(sh1.friday_open_time_1_end, sh1.friday_open_time_1_start, minute) end,
			case when time_diff(sh1.friday_open_time_2_end, sh1.friday_open_time_2_start, minute) is null then 0 else time_diff(sh1.friday_open_time_2_end, sh1.friday_open_time_2_start, minute) end)
			as friday_open_duration,

			safe_add(
			case when time_diff(sh1.saturday_open_time_1_end, sh1.saturday_open_time_1_start, minute) is null then 0 else time_diff(sh1.saturday_open_time_1_end, sh1.saturday_open_time_1_start, minute) end,
			case when time_diff(sh1.saturday_open_time_2_end, sh1.saturday_open_time_2_start, minute) is null then 0 else time_diff(sh1.saturday_open_time_2_end, sh1.saturday_open_time_2_start, minute) end)
			as saturday_open_duration,

			sh1.active_status,

		from `sonova-marketing`.data.dim_shop sh1

		) sh

		on ent.shop_id = sh.shop_id

	) unif

where
	unif.district_lead <> 'Test, Test'
	and unif.active_status = 'Active'