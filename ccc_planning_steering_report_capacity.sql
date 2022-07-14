select
	ind5.date,
	ind5.daynum,
	ind5.weeknum,
	ind5.max_capacity_lead_tickets_total,
	ind5.default_capacity_lead_tickets_total,
	ind5.expected_workload_lead_tickets,
	ind5.expected_workload_lead_tickets_cumulative,

from

	(select
		ind4.*,

		--cumulative calculation
		ind4.expected_workload_lead_tickets

		--backlog 6 days ago + overall no backlog last 5 days + backlog could not be handled past days
		+ case
			when
			lag(ind4.capacity_lead_tickets_gap, 6) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and
			lag(ind4.capacity_lead_tickets_gap, 5) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) <= 0
			and
			lag(ind4.capacity_lead_tickets_gap, 6) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 5) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) > 0

			then 		--add the remaining backlog
			lag(ind4.capacity_lead_tickets_gap, 6) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 5) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc)

			else 0 end

		--backlog 6 days ago + overall no backlog last 5 days + backlog could not be handled past days
		+ case
			when
			lag(ind4.capacity_lead_tickets_gap, 6) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 5) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) > 0

			then 		--add the full backlog
			lag(ind4.capacity_lead_tickets_gap, 6) over(order by ind4.weeknum, ind4.daynum asc)

			else 0 end

		--backlog 5 days ago + overall no backlog last 4 days + backlog could not be handled past days
		+ case
			when
			lag(ind4.capacity_lead_tickets_gap, 5) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and
			lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) <= 0
			and
			lag(ind4.capacity_lead_tickets_gap, 5) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) > 0

			then 		--add the remaining backlog
			lag(ind4.capacity_lead_tickets_gap, 5) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc)

			else 0 end

		--backlog 5 days ago + overall no backlog last 4 days + backlog could not be handled past days
		+ case
			when
			lag(ind4.capacity_lead_tickets_gap, 5) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) > 0

			then 		--add the full backlog
			lag(ind4.capacity_lead_tickets_gap, 5) over(order by ind4.weeknum, ind4.daynum asc)

			else 0 end

		--backlog 4 days ago + overall no backlog last 3 days + backlog could not be handled past days
		+ case
			when
			lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and
			lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) <= 0
			and
			lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) > 0

			then 		--add the remaining backlog
			lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc)

			else 0 end

		--backlog 4 days ago + overall no backlog last 3 days + backlog could not be handled past days
		+ case
			when
			lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) > 0

			then 		--add the full backlog
			lag(ind4.capacity_lead_tickets_gap, 4) over(order by ind4.weeknum, ind4.daynum asc)

			else 0 end

		--backlog 3 days ago + overall no backlog last 2 days + backlog could not be handled past days
		+ case
			when
			lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and
			lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) <= 0
			and
			lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) > 0

			then 		--add the remaining backlog
			lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc)

			else 0 end

		--backlog 3 days ago + overall no backlog last 2 days + backlog could not be handled past days
		+ case
			when
			lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) > 0

			then 		--add the full backlog
			lag(ind4.capacity_lead_tickets_gap, 3) over(order by ind4.weeknum, ind4.daynum asc)

			else 0 end

		--backlog 2 days ago + no backlog 1 day ago + backlog could not be handled past day
		+ case
			when
			lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) <= 0
			and
			lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) > 0

			then 		--add the remaining backlog
			lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)
			+ lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc)

			else 0 end

		--backlog 2 days ago + backlog 1 day ago + backlog could not be handled past day
		+ case
			when
			lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc) > 0
			and lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) > 0

			then 		--add the full backlog
			lag(ind4.capacity_lead_tickets_gap, 2) over(order by ind4.weeknum, ind4.daynum asc)

			else 0 end

		--day before backlog
		+ case
			when lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc) > 0
			then lag(ind4.capacity_lead_tickets_gap, 1) over(order by ind4.weeknum, ind4.daynum asc)

			else 0 end

		as expected_workload_lead_tickets_cumulative

	from

		(select
			ind3.*,
			safe_subtract(ind3.expected_workload_lead_tickets, ind3.default_capacity_lead_tickets_total) as capacity_lead_tickets_gap,

		from

			(select
				ind2.*,

				--new incoming lead ticket capacity; accounting for current backlog tickets
				round(
				safe_subtract(
				safe_divide(safe_divide(ind2.max_lead_ticket_working_hours, ind2.aht_lead_call_h), ind2.reached_rate),
				safe_divide(ind2.current_num_backlog_lead_tickets_day, ind2.reached_rate)),
				1)
				as max_capacity_lead_tickets_total,

				round(safe_subtract(
				safe_divide(safe_divide(ind2.default_lead_ticket_working_hours, ind2.aht_lead_call_h), ind2.reached_rate),
				safe_divide(ind2.current_num_backlog_lead_tickets_day, ind2.reached_rate)),
				1)
				as default_capacity_lead_tickets_total,

				round(ind2.total_lead_tickets_workload, 1) as expected_workload_lead_tickets,

			from


				(select
					fc.date,
					fc.daynum,
					format_date('%V', date(fc.date)) as weeknum,
					fc.forecast_qualified_lead_tickets_day,
					case when wl.current_num_backlog_lead_tickets_day is null then 0 else wl.current_num_backlog_lead_tickets_day end as current_num_backlog_lead_tickets_day,
					safe_add(fc.forecast_qualified_lead_tickets_day, case when wl.current_num_backlog_lead_tickets_day is null then 0 else wl.current_num_backlog_lead_tickets_day end) as total_lead_tickets_workload,
					fc.aht_lead_call_sec,
					fc.aht_lead_call_h,
					cast(0.8 as float64) as reached_rate,

					--w/ max share
					case
					when safe_multiply(safe_multiply(safe_multiply(wiw.planned_working_hours, wiw.max_lead_ticket_capacity_share), (1-fc.sickness_rate)), fc.occupancy) is null then 0
					else safe_multiply(safe_multiply(safe_multiply(wiw.planned_working_hours, wiw.max_lead_ticket_capacity_share), (1-fc.sickness_rate)), fc.occupancy) end
					as max_lead_ticket_working_hours,

					--w/ default share and quality/training/meeting/other shrinkage included
					case
					when safe_multiply(safe_multiply(safe_subtract(safe_multiply(wiw.planned_working_hours, wiw.default_lead_ticket_capacity_share), fc.shrinkage_hours), (1-fc.sickness_rate)), fc.occupancy) is null then 0
					else safe_multiply(safe_multiply(safe_subtract(safe_multiply(wiw.planned_working_hours, wiw.default_lead_ticket_capacity_share), fc.shrinkage_hours), (1-fc.sickness_rate)), fc.occupancy) end
					as default_lead_ticket_working_hours,

				from

					--forecasted incoming leads
					(select
						fc1.date,
						fc1.daynum,
						safe_multiply(fc1.forecast_qualified_lead_tickets_week, dis.daynum_share) as forecast_qualified_lead_tickets_day,
						fc1.aht_lead_call_sec,
						fc1.aht_lead_call_sec / 60 / 60 as aht_lead_call_h,
						fc1.sickness_rate,
						fc1.occupancy,
						case when fc1.daynum between 1 and 5 then safe_divide(fc1.shrinkage_hours_week, 5) else 0 end as shrinkage_hours,

					from

						(select
							cfc.date,
							cfc.reporting_week,
							case when extract(dayofweek from cfc.date ) = 1 then 7 else extract(dayofweek from cfc.date ) - 1 end as daynum,
							sum(cfc.fc_qualified_leads) over(partition by cfc.reporting_week) as forecast_qualified_lead_tickets_week,
							avg(cfc.fc_aht_s_appointment_call) over(partition by cfc.reporting_week) as aht_lead_call_sec,
							avg(cfc.fc_sickness_rate) over(partition by cfc.reporting_week) as sickness_rate,
							avg(cfc.fc_occupancy) over(partition by cfc.reporting_week) as occupancy,

							safe_add(
							safe_add(
							safe_multiply(cfc.fc_training_quality_h, cast(pcs.num_agents as float64)),
							safe_multiply(cfc.fc_meeting_h, cast(pcs.num_agents as float64))),
							cfc.fc_other_shrinkage_h)
							as shrinkage_hours_week,

						from `sonova-marketing`.reporting.ccc_forecast cfc

						left join `sonova-marketing`.`data`.fact_planned_ccc_staffing pcs
						on cfc.reporting_week = pcs.reporting_week

						) fc1


						left join

						--average weekday incoming lead distribution
						(select distinct
							ldt.daynum,
							count(distinct ldt.zendesk_ticket_id) over(partition by ldt.daynum) / count(ldt.zendesk_ticket_id) over() as daynum_share,

						from

							(select
							date(ftm.created_at) as ticket_date,
							ftm.zendesk_ticket_id,

							case
							when extract(dayofweek from ftm.created_at) = 1 then 7
							else extract(dayofweek from ftm.created_at) - 1 end
							as daynum,

							from `sonova-marketing`.data.fact_ticket_main ftm

							where
								ftm.test_flag = 0
								and ftm.lead_flag = 1
								and ftm.remote_lead_flag = 0
								and ftm.qualified_flag = 1

							) ldt

						where ldt.ticket_date between date_sub(current_date(), interval 29 day) and date_sub(current_date(), interval 1 day)

						) dis

						on fc1.daynum = dis.daynum

					) fc


					left join

					--available capacity
					(select
						dap.date,
						sum(dap.day_productivity_hours) as planned_working_hours,
						cast(0.8 as float64) as max_lead_ticket_capacity_share,
						cast(0.7 as float64) as default_lead_ticket_capacity_share,

					from `sonova-marketing`.`data`.fact_wiw_daily_absence_presence dap

					where
						dap.type = 'Presence'
						and dap.position = 'CCC Agent'
						and dap.date >= current_date()

					group by 1

					) wiw

					on fc.date = wiw.date


					left join

					--current open lead tickets workload
					(select
						wl3.date,
						wl3.daynum,
						case when wl3.daynum <= 6 then wl3.num_backlog_lead_tickets else 0 end as current_num_backlog_lead_tickets_day

					from

						(select
							wl2.*,
							case when extract(dayofweek from wl2.date ) = 1 then 7 else extract(dayofweek from wl2.date) - 1 end as daynum,

						from


							(select * from unnest(generate_date_array(current_date(), date_add(current_date(), interval 6 day))) as date

							cross join

							(select
								safe_divide(sum(tic.lead_flag), 6) as num_backlog_lead_tickets, 		--divide over next 7 days except sunday

							from

								(select *

								from

									--workload
									(select
										date(ftm.created_at) as ticket_date,
										ftm.lead_flag,
										date_diff(current_date(), date(ftm.created_at), day) as ticket_age_days,	--starting at 0 (today)

									from `sonova-marketing`.data.fact_ticket_main ftm

									where
										ftm.test_flag = 0
										and ftm.remote_lead_flag = 0
										and ftm.qualified_flag = 1
										and ftm.ticket_status in ('New', 'Open', 'Pending', 'On Hold')

									) tic1

								where tic1.ticket_age_days <= 6 	--only open tickets that are 7 days or older

								) tic

							) wl1

							) wl2

						) wl3

					) wl

					on fc.date = wl.date

					order by fc.date asc

				) ind2

			where ind2.date between current_date() and date_add(current_date(), interval 30 day) --only show the capacity for the coming 30 days

			) ind3

		) ind4

	) ind5