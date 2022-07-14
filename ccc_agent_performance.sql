select
	unif.reporting_date,
	unif.reporting_week,
	unif.reporting_month,
	unif.agent_name,
	unif.agent_email,
	unif.agent_status,
	unif.agent_team,
	unif.num_cust_sched_app_hour_target,
	sum(unif.call_duration_outbound_calls) as call_duration_outbound_calls,
	sum(unif.num_outbound_calls_total) as num_outbound_calls,
	sum(unif.num_outbound_calls_lead) as num_outbound_calls_lead,
	sum(unif.num_outbound_calls_reminder) as num_outbound_calls_reminder,
	sum(unif.num_outbound_calls_no_show) as num_outbound_calls_no_show,
	sum(unif.num_outbound_calls_follow_up) as num_outbound_calls_follow_up,
	sum(unif.num_outbound_calls_second_follow_up) as num_outbound_calls_second_follow_up,
	sum(unif.num_inbound_calls_total) as num_inbound_calls,
	sum(unif.num_cus_appointment) as num_cust_sched_app,
	sum(unif.num_cus_cancellation) as num_cust_cancellation,
	sum(unif.num_cus_no_show) as num_cust_no_show,
	sum(unif.num_cus_qscreen) as num_cust_qscreen,
	sum(unif.num_cus_sale) as num_cust_sale,
	sum(unif.cus_net_sales_hea_only) as cus_net_sales_hea_only,
	sum(unif.worked_hours) as worked_hours,
	sum(unif.worked_hours_month) as worked_hours_month,
	sum(unif.planned_hours) as planned_hours,
	sum(unif.sickness_hours) as sickness_hours,
	sum(unif.total_num_cus_appointment) as num_cust_sched_app_total,		--total on on that date, for RLS purposes
	sum(unif.total_num_cus_qscreen) as num_cust_qscreen_total,				--total on on that date, for RLS purposes
	sum(unif.total_num_cus_sale) as num_cust_sale_total,					--total on on that date, for RLS purposes
	sum(unif.total_worked_hours) as worked_hours_total,						--total on on that date, for RLS purposes
	max(case when unif.reporting_date >= date_sub(current_date(), interval 29 day) then unif.rolling_average_outbound_calls_hour else null end) as rolling_average_outbound_calls_hour,
	max(case when unif.reporting_date >= date_sub(current_date(), interval 29 day) then unif.rolling_average_outbound_calls_hour_team else null end) as rolling_average_outbound_calls_hour_team,
	max(case when unif.reporting_date >= date_sub(current_date(), interval 29 day) then unif.rolling_average_outbound_calls_hour_agent else null end) as rolling_average_outbound_calls_hour_agent,
	max(case when unif.reporting_date >= date_sub(current_date(), interval 29 day) then unif.rolling_average_num_cus_appointment_hour else null end) as rolling_average_num_cus_appointment_hour,
	max(case when unif.reporting_date >= date_sub(current_date(), interval 29 day) then unif.rolling_average_num_cus_appointment_hour_team else null end) as rolling_average_num_cus_appointment_hour_team,
	max(case when unif.reporting_date >= date_sub(current_date(), interval 29 day) then unif.rolling_average_num_cus_appointment_hour_agent else null end) as rolling_average_num_cus_appointment_hour_agent,
	max(case when unif.reporting_date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then unif.rolling_average_qualified_screening_ratio else null end) as rolling_average_qualified_screening_ratio,
	max(case when unif.reporting_date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then unif.rolling_average_qualified_screening_ratio_team else null end) as rolling_average_qualified_screening_ratio_team,
	max(case when unif.reporting_date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then unif.rolling_average_qualified_screening_ratio_agent else null end) as rolling_average_qualified_screening_ratio_agent,
	max(case when unif.reporting_date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then unif.rolling_average_qualified_screening_sales_ratio else null end) as rolling_average_qualified_screening_sales_ratio,
	max(case when unif.reporting_date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then unif.rolling_average_qualified_screening_sales_ratio_team else null end) as rolling_average_qualified_screening_sales_ratio_team,
 	max(case when unif.reporting_date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then unif.rolling_average_qualified_screening_sales_ratio_agent else null end) as rolling_average_qualified_screening_sales_ratio_agent,

from

	(select
       	
       	--count(ag.agent_name) over(partition by base.date, base.agent_name) as row_count --i.e. there are two statuses
       	--first_value(ag.status) over(partition by base.date, base.agent_name order by ag.status asc) as first_status --i.e. the first status is active or inactive
		case
		--two statuses, both inactive, only take one row
		when count(ag.agent_name) over(partition by base.date, base.agent_name) = 2 and first_value(ag.status) over(partition by base.date, base.agent_name order by ag.status asc) = 'Inactive' and row_number() over(partition by base.date, base.agent_name) = 1 then 0
		when count(ag.agent_name) over(partition by base.date, base.agent_name) = 2 and first_value(ag.status) over(partition by base.date, base.agent_name order by ag.status asc) = 'Inactive' and row_number() over(partition by base.date, base.agent_name) = 2 then 1
		--two status, active and inactive, only take the active row
		when count(ag.agent_name) over(partition by base.date, base.agent_name) = 2 and first_value(ag.status) over(partition by base.date, base.agent_name order by ag.status asc) = 'Active' and ag.status = 'Active' then 0
		when count(ag.agent_name) over(partition by base.date, base.agent_name) = 2 and first_value(ag.status) over(partition by base.date, base.agent_name order by ag.status desc) = 'Inactive' and ag.status = 'Inactive' then 1
		--else one status, do not exclude
		else 0 end
		as exclude_data_flag, --for agents with double entries switching to new email address/active to inactive
		
		base.date as reporting_date,
		base.reporting_week,
		base.reporting_month,
		base.agent_name,
		ag.agent_email,
		ag.status as agent_status,
		ag.team as agent_team,
		wiw.planned_hours,
		wiw.worked_hours,
		wiw.sickness_hours,
		cal.call_duration_outbound_calls,
		cal.num_outbound_calls_total,
		cal.num_outbound_calls_lead,
		cal.num_outbound_calls_reminder,
		cal.num_outbound_calls_no_show,
		cal.num_outbound_calls_follow_up,
		cal.num_outbound_calls_second_follow_up,
		cal.num_inbound_calls_total,
		func.num_cus_appointment,
		func.num_cus_cancellation,
		func.num_cus_no_show,
		func.num_cus_qscreen,
		func.num_cus_sale,
		func.cus_net_sales_hea_only,
		0.8 as num_cust_sched_app_hour_target,
		sum(wiw.worked_hours) over(partition by base.reporting_month, base.agent_name) as worked_hours_month,
		sum(func.num_cus_appointment) over(partition by base.date) as total_num_cus_appointment,
		sum(func.num_cus_qscreen) over(partition by base.date) as total_num_cus_qscreen,
		sum(func.num_cus_sale) over(partition by base.date) as total_num_cus_sale,
		sum(wiw.worked_hours) over(partition by base.date) as total_worked_hours,

		safe_divide(sum(cal.num_outbound_calls_total) over(partition by(case when base.date >= date_sub(current_date(), interval 29 day) then 1 else null end))
		, sum(wiw.worked_hours) over(partition by(case when base.date >= date_sub(current_date(), interval 29 day) then 1 else null end)))
		as rolling_average_outbound_calls_hour,

		safe_divide(sum(cal.num_outbound_calls_total) over(partition by(case when base.date >= date_sub(current_date(), interval 29 day) then 1 else null end), ag.team)
		, sum(wiw.worked_hours) over(partition by(case when base.date >= date_sub(current_date(), interval 29 day) then 1 else null end), ag.team))
		as rolling_average_outbound_calls_hour_team,

		safe_divide(sum(cal.num_outbound_calls_total) over(partition by(case when base.date >= date_sub(current_date(), interval 29 day) then 1 else null end), ag.agent_name)
		, sum(wiw.worked_hours) over(partition by(case when base.date >= date_sub(current_date(), interval 29 day) then 1 else null end), ag.agent_name))
		as rolling_average_outbound_calls_hour_agent,

		safe_divide(sum(func.num_cus_appointment) over(partition by(case when base.date >= date_sub(current_date(), interval 29 day) then 1 else null end))
		, sum(wiw.worked_hours) over(partition by(case when base.date >= date_sub(current_date(), interval 29 day) then 1 else null end)))
		as rolling_average_num_cus_appointment_hour,

		safe_divide(sum(func.num_cus_appointment) over(partition by(case when base.date >= date_sub(current_date(), interval 29 day) then 1 else null end), ag.team)
		, sum(wiw.worked_hours) over(partition by(case when base.date >= date_sub(current_date(), interval 29 day) then 1 else null end), ag.team))
		as rolling_average_num_cus_appointment_hour_team,

		safe_divide(sum(func.num_cus_appointment) over(partition by(case when base.date >= date_sub(current_date(), interval 29 day) then 1 else null end), ag.agent_name)
		, sum(wiw.worked_hours) over(partition by(case when base.date >= date_sub(current_date(), interval 29 day) then 1 else null end), ag.agent_name))
		as rolling_average_num_cus_appointment_hour_agent,

		safe_divide(sum(func.num_cus_qscreen) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end))
		, sum(func.num_cus_appointment) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end)))
		as rolling_average_qualified_screening_ratio,

		safe_divide(sum(func.num_cus_qscreen) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end), ag.team)
		, sum(func.num_cus_appointment) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end), ag.team))
		as rolling_average_qualified_screening_ratio_team,

     	safe_divide(sum(func.num_cus_qscreen) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end), ag.agent_name)
		, sum(func.num_cus_appointment) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end), ag.agent_name))
		as rolling_average_qualified_screening_ratio_agent,

		safe_divide(sum(func.num_cus_sale) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end))
		, sum(func.num_cus_qscreen) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end)))
		as rolling_average_qualified_screening_sales_ratio,

		safe_divide(sum(func.num_cus_sale) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end), ag.team)
		, sum(func.num_cus_qscreen) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end), ag.team))
		as rolling_average_qualified_screening_sales_ratio_team,

  		case
    	when sum(func.num_cus_sale) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end), ag.agent_name) = 0 then 0
		when sum(func.num_cus_qscreen) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end), ag.agent_name) = 0 then 0
		else safe_divide(sum(func.num_cus_sale) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end), ag.agent_name)
		, sum(func.num_cus_qscreen) over(partition by(case when base.date between date_sub(current_date(), interval 50 day) and date_sub(current_date(), interval 22 day) then 1 else null end), ag.agent_name)) end
		as rolling_average_qualified_screening_sales_ratio_agent,

	from

		--base
		(select *

		from

			(select

				dts1.date,
				concat(cast(format_date('%G', date(dts1.date)) as string), '-W', cast(format_date('%V', date(dts1.date)) as string)) as reporting_week,
				cast(format_date('%Y-%m', dts1.date) as string) as reporting_month,

			from

				(select *

				from unnest(generate_date_array('2019-08-01', date_sub(current_date(), interval 1 day), interval 1 day)) as date) dts1

			) dts

			cross join

			unnest(

			array

				(select distinct concat(dam.first_name, ' ', dam.last_name) as agent_name,

				from `sonova-marketing`.`data`.dim_agent_main dam

				where dam.status <> 'Exclude')

			) as agent_name

		) base


		left join

		(select distinct
			concat(dam.first_name, ' ', dam.last_name) as agent_name,
			dam.email as agent_email,
			dam.status,
			dam.team

		from `sonova-marketing`.`data`.dim_agent_main dam

		) ag

		on base.agent_name = ag.agent_name


		left join

		(select
			dap.date,
			dap.employee,
			sum(dap.day_productivity_hours) as planned_hours, 								--all hours, absence + presence
			sum(case when dap.type = 'Presence' then dap.day_productivity_hours end) as worked_hours,
			sum(case when dap.type = 'Absence' and dap.absence_type = 'Sick' then dap.day_productivity_hours end) as sickness_hours

		from `sonova-marketing`.`data`.fact_wiw_daily_absence_presence dap

		group by 1,2

		) wiw

		on base.date = wiw.date
		and base.agent_name = wiw.employee


		left join

		(select
			fcm.agent_name,
			fcm.call_date,
			count(case when fcm.call_type = 'Inbound' then fcm.call_id end) as num_inbound_calls_total,
			count(case when fcm.call_type = 'Outbound' then fcm.call_id end) as num_outbound_calls_total,
			count(case when fcm.call_type = 'Outbound' and ccp.ticket_type = 'Lead' then fcm.call_id end) as num_outbound_calls_lead,
			count(case when fcm.call_type = 'Outbound' and ccp.ticket_type = 'Reminder' then fcm.call_id end) as num_outbound_calls_reminder,
			count(case when fcm.call_type = 'Outbound' and ccp.ticket_type = 'No-show' then fcm.call_id end) as num_outbound_calls_no_show,
			count(case when fcm.call_type = 'Outbound' and ccp.ticket_type = 'Follow-up' then fcm.call_id end) as num_outbound_calls_follow_up,
			count(case when fcm.call_type = 'Outbound' and ccp.ticket_type = 'Second Follow-up' then fcm.call_id end) as num_outbound_calls_second_follow_up,
			sum(case when fcm.call_type = 'Outbound' then fcm.call_total_time end) as call_duration_outbound_calls,

		from `sonova-marketing`.`data`.fact_call_main fcm
		left join `sonova-marketing`.`reporting`.ccc_call_performance ccp
		on fcm.call_id = ccp.call_id
		and fcm.call_date = ccp.call_date
		and fcm.call_start_time = ccp.call_start_time		--some calls with identical call_id, therefore also match on date and time

		group by 1,2

		) cal

		on base.date = cal.call_date
		and base.agent_name = cal.agent_name


		left join

		(select
			func2.agent_name,
			func2.appointment_start,
			count(distinct func2.customer_id) as num_cus_appointment,
			count(distinct case when func2.cancelled_flag = 1 then func2.customer_id else null end) as num_cus_cancellation,
			count(distinct case when func2.no_show_flag = 1 then func2.customer_id else null end) as num_cus_no_show,
			count(distinct case when func2.has_qscreen = 1 then func2.customer_id else null end) as num_cus_qscreen,
			count(distinct case when func2.has_sale = 1 then func2.customer_id else null end) as num_cus_sale,
			sum(func2.cus_net_sales) as cus_net_sales,
			sum(func2.cus_net_sales_hea_only) as cus_net_sales_hea_only,

		from

			(select
				func1.*,
				max(case when sal.first_invoice_date is not null then 1 else 0 end) as has_sale,
				sum(sal.total_net_sales_in_eur) as cus_net_sales,
				sum(sal.total_net_sales_hea_in_eur) as cus_net_sales_hea_only,

			from

				(select distinct
					app.customer_id,
					app.agent_name,
					app.appointment_start,
					app.next_valid_appointment_created_date,

					max(case
					when nap.cancelled_flag = 1 then 1
					when out.cancelled_flag = 1 then 1
					else 0 end)
					as cancelled_flag,

					max(nap.no_show_flag) as no_show_flag,
					max(scr.qualified_flag) as has_qscreen,

				from

					(select 
						app6.* except(valid_appointment_flag),
						lead(app6.appointment_start, 1) over(partition by app6.customer_id order by app6.appointment_start asc) as next_valid_appointment_created_date,

					from

						(select
							app5.* except(appointment_update_rank_asc, appointment_id, previous_appointment_created_date, appointment_count_reset_date),

						    case 
						    when app5.previous_appointment_created_date is null then 1
						    when app5.appointment_start >= app5.appointment_count_reset_date then 1 
						    else 0 end 
						    as valid_appointment_flag,

						from

							(select 
								app4.*,
								lag(app4.appointment_start, 1) over(partition by app4.customer_id order by app4.appointment_start asc) as previous_appointment_created_date,
							    date_add(lag(app4.appointment_start, 1) over(partition by app4.customer_id order by app4.appointment_start asc), interval 1 year) as appointment_count_reset_date,

							from

								(select 
									app3.*,
									row_number() over(partition by app3.appointment_id order by app3.updated_at asc) as appointment_update_rank_asc,

								from

									(select
										app2.*,
										concat(cast(app2.customer_id as string), '-', cast(app2.appointment_date as string), '-', cast(app2.shop_id as string)) as appointment_id,

									from

										(select
										 	app1.* except(update_rank_desc, update_rank_asc, appointment_start, customer_id, appointment_date, shop_id),
										 	max(case when app1.update_rank_asc = 1 then app1.appointment_start end) over(partition by app1.zendesk_ticket_id) as appointment_start,		--get first updated date to set as appointment_start
											max(case when app1.update_rank_desc = 1 then app1.customer_id end) over(partition by app1.zendesk_ticket_id) as customer_id, 				--get last entered customer id to account for changing customer id later on
											max(case when app1.update_rank_desc = 1 then app1.appointment_date end) over(partition by app1.zendesk_ticket_id) as appointment_date,
											max(case when app1.update_rank_desc = 1 then app1.shop_id end) over(partition by app1.zendesk_ticket_id) as shop_id,
											
										from

											(select distinct
												ftu.zendesk_ticket_id,
												ftu.agent_name,
												ftu.created_at,
												ftu.updated_at,
												date(ftu.updated_at) as appointment_start,
												ftu.customer_id,
												ftu.appointment_date,
												extract(time from ftu.appointment_start_time) as appointment_time,
												ftu.shop_id,
												row_number() over(partition by ftu.zendesk_ticket_id order by ftu.updated_at desc) as update_rank_desc,
												row_number() over(partition by ftu.zendesk_ticket_id order by ftu.updated_at asc) as update_rank_asc,

											from `sonova-marketing`.`data`.fact_ticket_update ftu

											where
												ftu.test_flag = 0
												and ftu.lead_flag = 1
												and ftu.contact_reason in ('Hearing Test & Consultation', 'Hearing Test Only')
												and ftu.appointment_date is not null
												and ftu.customer_id is not null
												and ftu.agent_name != ''
												and ftu.agent_name is not null

											) app1

										) app2

									) app3

								) app4

								where app4.appointment_update_rank_asc = 1

							) app5

						) app6 

					where app6.valid_appointment_flag = 1

					) app


					left join

					(select distinct
						ftu.customer_id,
						ftu.appointment_date,
						extract(time from ftu.appointment_start_time) as appointment_time,
						ftu.shop_id,

						max(case when coalesce(
						ftu.contact_result in ('appointment_cancelledcorona', 'appointment_cancelled'),
						ftu.contact_reason_raw like '%Termin abgesagt%')
						then 1
						else 0 end)
						as cancelled_flag,

					from `sonova-marketing`.`data`.fact_ticket_update ftu

					where
						ftu.test_flag = 0
						and ftu.qualified_flag = 1
						and ftu.appointment_date is not null
						and ftu.customer_id is not null

					group by 1,2,3,4

					) out

					on app.customer_id = out.customer_id
					and app.appointment_date = out.appointment_date
					and app.appointment_time = out.appointment_time


					left join

					(select
						fam.customer_id,
						date(fam.appointment_created_at) as appointment_start,
						fam.appointment_date,
						extract(time from fam.appointment_start_time) as appointment_time,
						fam.shop_id,
						fam.cancelled_flag,
						fam.no_show_flag,

					from `sonova-marketing`.`data`.fact_appointment_main fam

					where
						fam.post_lead_flag = 1
						and fam.appointment_type = 'Sales'

					) nap

					on app.customer_id = nap.customer_id
					and app.appointment_date = nap.appointment_date
					and app.appointment_time = nap.appointment_time
					and app.shop_id = nap.shop_id


					left join

					(select
						fsm.customer_id,
						fsm.screening_date,
						fsm.qualified_flag,

					from `sonova-marketing`.`data`.fact_screening_main fsm

					where fsm.post_lead_flag = 1

					) scr

					on app.customer_id = scr.customer_id
					and case
					when app.next_valid_appointment_created_date is null then scr.screening_date >= app.appointment_start
					when app.next_valid_appointment_created_date is not null then scr.screening_date between app.appointment_start and app.next_valid_appointment_created_date-1 end

				group by 1,2,3,4

				) func1


				left join

				(select
					fsa.customer_id,
					fsa.transaction_key,
					fsa.first_invoice_date,
					fsa.total_net_sales_in_eur,
					fsa.total_net_sales_hea_in_eur,

				from `sonova-marketing`.`data`.fact_sales_main fsa

				where
					fsa.post_lead_flag = 1
					and fsa.hea_quantity_invoiced > 0

				) sal

				on func1.customer_id = sal.customer_id
				and case
				when func1.next_valid_appointment_created_date is null then sal.first_invoice_date >= func1.appointment_start
				when func1.next_valid_appointment_created_date is not null then sal.first_invoice_date between func1.appointment_start and func1.next_valid_appointment_created_date-1 end

			group by 1,2,3,4,5,6,7

			) func2

		group by 1,2

		) func

		on base.agent_name = func.agent_name
		and base.date = func.appointment_start

	) unif

where unif.exclude_data_flag = 0

group by 1,2,3,4,5,6,7,8