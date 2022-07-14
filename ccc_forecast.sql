select
	unif4.*,
	unif4.fc_appointment_calls + unif4.fc_reminder_calls + unif4.fc_noshow_calls + unif4.fc_followup_calls + unif4.fc_second_followup_calls + unif4.fc_call_attempts as fc_outbound_calls,
	(unif4.fc_appointment_calls + unif4.fc_reminder_calls + unif4.fc_noshow_calls + unif4.fc_followup_calls + unif4.fc_second_followup_calls + unif4.fc_call_attempts) * unif4.fc_inbound_call_ratio as fc_inbound_calls,

	(unif4.fc_aht_s_appointment_call * unif4.fc_appointment_calls + unif4.fc_aht_s_reminder_call * unif4.fc_reminder_calls + 
	unif4.fc_aht_s_noshow_call * unif4.fc_noshow_calls + unif4.fc_aht_s_followup_call * unif4.fc_followup_calls + 
	unif4.fc_aht_s_second_followup_call * unif4.fc_second_followup_calls + unif4.fc_aht_s_call_attempt * unif4.fc_call_attempts +
	unif4.fc_aht_s_inbound_call * ((unif4.fc_appointment_calls + unif4.fc_reminder_calls + unif4.fc_noshow_calls + unif4.fc_followup_calls + unif4.fc_second_followup_calls + unif4.fc_call_attempts) * unif4.fc_inbound_call_ratio)
	) / 60 / 60 
	as fc_workload_h,

	safe_divide(
	(unif4.fc_aht_s_appointment_call * unif4.fc_appointment_calls + unif4.fc_aht_s_reminder_call * unif4.fc_reminder_calls +
	unif4.fc_aht_s_noshow_call * unif4.fc_noshow_calls + unif4.fc_aht_s_followup_call * unif4.fc_followup_calls +
	unif4.fc_aht_s_second_followup_call * unif4.fc_second_followup_calls + unif4.fc_aht_s_call_attempt * unif4.fc_call_attempts +
	unif4.fc_aht_s_inbound_call * ((unif4.fc_appointment_calls + unif4.fc_reminder_calls + unif4.fc_noshow_calls + unif4.fc_followup_calls + unif4.fc_second_followup_calls + unif4.fc_call_attempts) * unif4.fc_inbound_call_ratio)
	) / 60 / 60
	/ unif4.fc_occupancy
	+ unif4.act_num_agents * unif4.fc_training_quality_h + unif4.act_num_agents * unif4.fc_meeting_h + unif4.fc_other_shrinkage_h
	, (1-(unif4.fc_sickness_rate + unif4.fc_holiday_rate)))
	/ unif4.act_fte_h_day
	as fc_required_fte,

from

	(select
		unif3.*,
		(unif3.fc_appointment_calls + unif3.fc_reminder_calls + unif3.fc_noshow_calls + unif3.fc_followup_calls + unif3.fc_second_followup_calls) * unif3.fc_call_attempt_ratio as fc_call_attempts,

	from
		
		(select
			unif2.*,
			ifnull(lead(unif2.fc_qualified_leads, 8) over(order by unif2.date desc) * unif2.fc_cr_lead_ticket_called_reached_to_scheduled_appointment * unif2.fc_call_reached_share, 0) as fc_reminder_calls,	--assumption: appointment 9 days after call, reminder 1 day before appointment
			ifnull(lead(unif2.fc_qualified_leads, 10) over(order by unif2.date desc) * unif2.fc_cr_lead_ticket_called_reached_to_scheduled_appointment * unif2.fc_no_show_rate * unif2.fc_call_reached_share, 0) as fc_noshow_calls,	--assumption: no-show 1 day after analytics device appointment
			ifnull(lead(unif2.fc_qualified_leads, 10) over(order by unif2.date desc) * unif2.fc_cr_lead_ticket_called_reached_to_scheduled_appointment * unif2.fc_call_reached_share, 0) as fc_followup_calls, 
			ifnull(lead(unif2.fc_qualified_leads, 20) over(order by unif2.date desc) * unif2.fc_cr_lead_ticket_called_reached_to_scheduled_appointment * unif2.fc_cr_screening_to_qscreening * unif2.fc_cr_qscreening_to_trial * unif2.fc_call_reached_share, 0) as fc_second_followup_calls,  --assumption: second appointment for bte 10 days after, second follow-up 1 day thereafter

		from

			(select 
				unif.*,
				unif.fc_marketing_costs / unif.fc_cost_per_lead as fc_leads,
				unif.fc_marketing_costs / unif.fc_cost_per_lead * unif.fc_cr_lead_to_qlead as fc_qualified_leads,
				unif.fc_marketing_costs / unif.fc_cost_per_lead * unif.fc_lead_to_lead_tickets_called_ratio * unif.fc_lead_tickets_called_reached_share as fc_appointment_calls,

			from

				(with actuals as

					(select
						dat.date,
						dat.date_age,
						dat.fc_flag,
						dat.reporting_week,
						dat.reporting_month,
						mar.act_marketing_costs,
						ld.* except(date),
						jrn.* except(date),
						cal.* except(call_date),
						cal1.* except(date),
						ifnull(shr.act_productivity_hours, 0) as act_productivity_hours,
						ifnull(shr.act_sickness_hours, 0) as act_sickness_hours,
						ifnull(shr.act_holiday_hours, 0) as act_holiday_hours,
						shr.act_sickness_rate,
						shr.act_holiday_rate,
						stf.* except(reporting_week),
						safe_divide(mar.act_marketing_costs, ld.act_leads) as act_cost_per_lead,
						safe_divide(ld.act_qualified_leads, ld.act_leads) as act_cr_lead_to_qlead,
						safe_divide(jrn.act_customers_no_show, jrn.act_customers_scheduled_appointment) as act_no_show_rate,
						safe_divide(jrn.act_customers_qscreening, jrn.act_customers_screening) as act_cr_screening_to_qscreening,
						safe_divide(jrn.act_customers_trial, jrn.act_customers_qscreening) as act_cr_qscreening_to_trial,
						safe_divide(cal1.act_lead_tickets_called, ld.act_leads) as act_lead_to_lead_tickets_called_ratio,
						safe_divide(cal1.act_lead_tickets_called_reached, cal1.act_lead_tickets_called) as act_lead_tickets_called_reached_share,
						safe_divide(cal1.act_lead_tickets_called_reached_w_appointment, cal1.act_lead_tickets_called_reached) as act_cr_lead_ticket_called_reached_to_scheduled_appointment,
						safe_divide(cal.act_outbound_call_attempts, cal.act_outbound_calls_reached) as act_call_attempt_ratio,
						safe_divide(cal.act_inbound_calls_reached, cal.act_outbound_calls) as act_inbound_call_ratio,

						case 
						when dat.fc_flag = 1 then null 
						else safe_divide(sum(mar.act_marketing_costs) over(order by dat.date rows between 28 preceding and 1 preceding), sum(ld.act_leads) over(order by dat.date rows between 28 preceding and 1 preceding)) end 
						as act_cost_per_lead_last_4_weeks,

						case 
						when dat.fc_flag = 1 then null 
						else safe_divide(sum(ld.act_qualified_leads) over(order by dat.date rows between 28 preceding and 1 preceding), sum(ld.act_leads) over(order by dat.date rows between 28 preceding and 1 preceding)) end
						as act_cr_lead_to_qlead_last_4_weeks,
						
						case 
						when dat.fc_flag = 1 then null 
						else safe_divide(sum(cal1.act_lead_tickets_called_reached_w_appointment) over(order by dat.date rows between 28 preceding and 1 preceding), sum(cal1.act_lead_tickets_called_reached) over(order by dat.date rows between 28 preceding and 1 preceding)) end
						as act_cr_lead_ticket_called_reached_to_scheduled_appointment_last_4_weeks,
						
						case 
						when dat.fc_flag = 1 then null 
						else safe_divide(sum(jrn.act_customers_no_show) over(order by dat.date rows between 49 preceding and 22 preceding), sum(jrn.act_customers_scheduled_appointment) over(order by dat.date rows between 28 preceding and 1 preceding)) end
						as act_no_show_rate_4_weeks_shifted,
						
						case 
						when dat.fc_flag = 1 then null 
						else safe_divide(sum(jrn.act_customers_qscreening) over(order by dat.date rows between 28 preceding and 1 preceding), sum(jrn.act_customers_screening) over(order by dat.date rows between 28 preceding and 1 preceding)) end
						as act_cr_screening_to_qscreening_last_4_weeks,
						
						case 
						when dat.fc_flag = 1 then null 
						else safe_divide(sum(jrn.act_customers_trial) over(order by dat.date rows between 28 preceding and 1 preceding), sum(jrn.act_customers_qscreening) over(order by dat.date rows between 28 preceding and 1 preceding)) end
						as act_cr_qscreening_to_trial_last_4_weeks,

						case 
						when dat.fc_flag = 1 then null 
						else safe_divide(sum(cal1.act_lead_tickets_called) over(order by dat.date rows between 28 preceding and 1 preceding), sum(ld.act_leads) over(order by dat.date rows between 28 preceding and 1 preceding)) end
						as act_lead_to_lead_tickets_called_ratio_last_4_weeks, 	

						case 
						when dat.fc_flag = 1 then null 
						else safe_divide(sum(cal1.act_lead_tickets_called_reached) over(order by dat.date rows between 28 preceding and 1 preceding), sum(cal1.act_lead_tickets_called) over(order by dat.date rows between 28 preceding and 1 preceding)) end
						as act_lead_tickets_called_reached_share_last_4_weeks, 					

						case 
						when dat.fc_flag = 1 then null 
						else safe_divide(sum(cal.act_outbound_call_attempts) over(order by dat.date rows between 28 preceding and 1 preceding), sum(cal.act_outbound_calls_reached) over(order by dat.date rows between 28 preceding and 1 preceding)) end
						as act_call_attempt_ratio_last_4_weeks,

						case 
						when dat.fc_flag = 1 then null 
						else safe_divide(sum(cal.act_outbound_calls_reached) over(order by dat.date rows between 28 preceding and 1 preceding), sum(cal.act_outbound_calls) over(order by dat.date rows between 28 preceding and 1 preceding)) end
						as act_call_reached_share_last_4_weeks,
						
						case 
						when dat.fc_flag = 1 then null 
						else safe_divide(sum(cal.act_inbound_calls_reached) over(order by dat.date rows between 28 preceding and 1 preceding), sum(cal.act_outbound_calls) over(order by dat.date rows between 28 preceding and 1 preceding)) end
						as act_inbound_call_ratio_last_4_weeks,

						case 
						when dat.fc_flag = 1 then null 
						else safe_divide(sum(shr.act_sickness_hours) over(order by dat.date rows between 28 preceding and 1 preceding), sum(shr.act_productivity_hours) over(order by dat.date rows between 28 preceding and 1 preceding)) end
						as act_sickness_rate_last_4_weeks,

					from

							(select
								predates.date,
								concat(cast(format_date('%G', predates.date) as string), '-W', cast(format_date('%V', predates.date) as string)) as reporting_week,
								cast(format_date('%Y-%m', predates.date) as string) as reporting_month,
								date_diff(current_date(), predates.date, day) as date_age,
								case when predates.date < current_date() then 0 else 1 end as fc_flag,

							from (select * from unnest(generate_date_array('2020-09-01', date_add(current_date(), interval 12 month))) as date) predates

							) dat


							left join

							(select
								fdm.date,
								sum(fdm.marketing_costs_in_eur) as act_marketing_costs

							from `sonova-marketing.data.fact_daily_marketing_activity` fdm

							where
								fdm.date < current_date ()
								and fdm.campaign not like'%REMOTEFITTING%'

							group by 1

							) mar

							on dat.date = mar.date


							left join

							(select
								ld1.date,
								ld1.score_10_leads as act_qualified_leads,
								ifnull(ld1.score_0_leads, 0) + ifnull(ld1.score_10_leads, 0) as act_leads,

							from

								(select
									date(flm.lead_date) as date,
									count(distinct case when flm.qualified_flag = 1 then coalesce(flm.uuid, flm.clang_customer_id) end) as score_10_leads,
									count(distinct case when flm.qualified_flag = 0 then coalesce(flm.uuid, flm.clang_customer_id) end) as score_0_leads,

								from `sonova-marketing`.`data`.fact_lead_main flm

								where
									flm.update_rank_desc = 1
									and date(flm.lead_date) < current_date ()

								group by 1

								) ld1

							) ld

							on dat.date = ld.date


							left join

							(select
								app.date,
								count(distinct app.customer_id) as act_customers_scheduled_appointment,
								count(distinct case when nsh.no_show_flag = 1 then nsh.customer_id end) as act_customers_no_show,
								count(distinct scr.customer_id) as act_customers_screening,
								count(distinct case when scr.qualified_flag = 1 then scr.customer_id end) as act_customers_qscreening,
								count(distinct case when trl.trial_flag = 1 then trl.customer_id end) as act_customers_trial,

							from

								(select distinct
									app1.date,
									app1.customer_id,

								from

									(select distinct
										date(ftu.created_at) as date,
										ftu.customer_id,
										ftu.appointment_date,
										row_number() over (partition by ftu.customer_id order by ftu.updated_at asc) as appointment_update_rank_asc,

									from `sonova-marketing`.`data`.fact_ticket_update ftu

									where
										date(ftu.created_at) < current_date()
										and ftu.test_flag = 0
										and ftu.lead_flag = 1
										and ftu.contact_reason in ('Hearing Test & Consultation', 'Hearing Test Only')
										and ftu.appointment_date is not null
										and ftu.customer_id is not null

									) app1

								where app1.appointment_update_rank_asc = 1

								) app


								left join

								(select distinct
									app.customer_id,

									max(case
									when nsh.no_show_flag = 0 and zns.no_show_flag = 0 then 0				--both 0 = 0
									when nsh.no_show_flag = 1 or zns.no_show_flag = 1 then 1				--either 1 then 1
									when nsh.no_show_flag is null
									and (scr.screening_date is null or scr.screening_date < nsh.appointment_date)	--there is an appointment date which is eligble for no show
									and date_add(nsh.appointment_date, interval 7 day) < current_date() 			--and this is > 7 days in the past (and there is no screening date)
									then 1
									else 0 end)
									as no_show_flag,

								from

									(select
										ftm.customer_id,
										ftm.appointment_date,
										extract(time from ftm.appointment_start_time) as appointment_time,
										ftm.shop_id,

									from `sonova-marketing`.`data`.fact_ticket_main ftm

									where
										ftm.test_flag = 0
										and ftm.lead_flag = 1
										and ftm.contact_reason in ('Hearing Test & Consultation', 'Hearing Test Only')
										and ftm.remote_lead_flag = 0
										and ftm.appointment_date is not null

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

										min(case
										when ftu.contact_result in
										('appointment_cancelledcorona', 'appointment_cancelled',										--not cancelled = 0
										'follow_up_appointment_show', 'follow_up_no_hearingissue', 'follow_up_first_ent') then 0 		--show indicator = 0
										when ftu.contact_result like 'appointment_rescheduled%' then 0									--not rescheduled = 0
										when ftu.contact_result in ('follow_up_no_show') then 1											--if not that or null and follow_up_no_show = 1 then 1 (i.e. appointment not cancelled or rescheduled beforehand)
										else 0
										end)
										as no_show_flag,

									from `sonova-marketing`.`data`.fact_ticket_update ftu

									where
										ftu.test_flag = 0
										and ftu.appointment_date is not null
										and ftu.customer_id is not null

									group by 1,2,3,4

									) zns

									on app.customer_id = zns.customer_id
									and app.appointment_date = zns.appointment_date
									and app.appointment_time = zns.appointment_time


									left join

									(select
										fam.customer_id,
										fam.appointment_date,
										extract(time from fam.appointment_start_time) as appointment_time,
										fam.shop_id,

										case when fam.attendance_code = 'ABG' then 1 else 0 end as cancelled_flag,

										case
										when fam.attendance_code is null then null
										when fam.attendance_code in ('SHO', 'FOL', 'ABG', 'ABB', 'HNO') then 0
										when fam.attendance_code = 'NSH' and fam.berlin_ccc_agent_flag = 1 then 1
										else null end
										as no_show_flag,

									from `sonova-marketing`.`data`.fact_appointment_main fam

									where
										fam.post_lead_flag = 1
										and fam.appointment_type = 'Sales'

									) nsh

									on app.customer_id = nsh.customer_id
									and app.appointment_date = nsh.appointment_date
									and app.appointment_time = nsh.appointment_time
									and app.shop_id = nsh.shop_id


									left join

									(select
										dc.customer_id,
										fsm.screening_date,

									from `sonova-marketing`.`data`.fact_screening_main fsm
									left join `sonova-marketing`.`data`.dim_customer dc
									on fsm.customer_key = dc.customer_key

									where fsm.post_lead_flag = 1

									) scr

									on app.customer_id = scr.customer_id

									group by 1

								) nsh

								on app.customer_id = nsh.customer_id


								left join

								(select distinct
									dc.customer_id,
									max(fsm.qualified_flag) as qualified_flag,

								from `sonova-marketing`.data.fact_screening_main fsm
								left join `sonova-marketing`.data.dim_customer dc
								on fsm.customer_key = dc.customer_key

								where fsm.post_lead_flag = 1

								group by 1

								) scr

								on app.customer_id = scr.customer_id


								left join

								(select distinct
									fom.customer_id,
									max(case when fom.original_hea_quantity > 0 and fom.trial_flag = 1 then 1 else 0 end) as trial_flag

								from `sonova-marketing`.data.fact_order_main fom

								where fom.post_lead_flag = 1

								group by 1

								) trl

								on app.customer_id = trl.customer_id

								group by 1

							) jrn

							on dat.date = jrn.date


							left join

							(select
								ccp.call_date,
								count(case when ccp.call_type = 'Outbound' then ccp.call_id end) as act_outbound_calls,
								count(case when ccp.call_type = 'Outbound' and ccp.interaction_flag = 0 then ccp.call_id end) as act_outbound_call_attempts,
								count(case when ccp.call_type = 'Outbound' and ccp.interaction_flag = 1 then ccp.call_id end) as act_outbound_calls_reached,
								count(case when ccp.call_type = 'Inbound' and ccp.interaction_flag = 1 then ccp.call_id end) as act_inbound_calls_reached,
								
							from `sonova-marketing`.reporting.ccc_call_performance ccp

							where ccp.agent_name <> 'External'

							group by 1

							) cal

							on dat.date = cal.call_date


							left join

							(select
								cpm.date,
								count(case when cpm.ticket_type = 'Lead' then cpm.zendesk_ticket_id end) as act_lead_tickets_called,
								count(case when cpm.ticket_type = 'Lead' and cpm.interaction_flag = 1 then cpm.zendesk_ticket_id end) as act_lead_tickets_called_reached,	
								count(case when cpm.ticket_type = 'Lead' and cpm.interaction_flag = 1 and cpm.appointment_flag = 1 then cpm.zendesk_ticket_id end) as act_lead_tickets_called_reached_w_appointment						

							from `sonova-marketing`.reporting.ccc_call_performance_main cpm

							where cpm.duplicate_flag = 0

							group by 1

							) cal1

							on dat.date = cal1.date


							left join

							(select
								sha.date,
								sha.total_productivity_hours as act_productivity_hours,
								sha.total_sickness_hours as act_sickness_hours,
								sha.total_holiday_hours as act_holiday_hours,
								sha.sickness_rate as act_sickness_rate,
								sha.holiday_rate as act_holiday_rate,

							from `sonova-marketing`.`data`.fact_ccc_shrinkage_actuals sha

							) shr

							on dat.date = shr.date


							left join

							(select
								fis.reporting_week,
								fis.productive_fte as act_productive_fte,
								fis.num_agents as act_num_agents

							from `sonova-marketing`.data.fact_planned_ccc_staffing fis

							) stf

							on dat.reporting_week = stf.reporting_week
					)


					select
						actuals.*,
						mfc.fc_marketing_costs,
						prd.* except(reporting_week),

						case 
						when actuals.fc_flag = 1 then first_value(actuals.act_cost_per_lead_last_4_weeks) over(order by actuals.act_cost_per_lead_last_4_weeks is null, actuals.date desc)
						else actuals.act_cost_per_lead_last_4_weeks end
						as fc_cost_per_lead,

						case 
						when actuals.fc_flag = 1 then first_value(actuals.act_cr_lead_to_qlead_last_4_weeks) over(order by actuals.act_cr_lead_to_qlead_last_4_weeks is null, actuals.date desc)
						else actuals.act_cr_lead_to_qlead_last_4_weeks end
						as fc_cr_lead_to_qlead,

						case 
						when actuals.fc_flag = 1 then first_value(actuals.act_no_show_rate_4_weeks_shifted) over(order by actuals.act_no_show_rate_4_weeks_shifted is null, actuals.date desc)
						else actuals.act_no_show_rate_4_weeks_shifted end
						as fc_no_show_rate,

						case 
						when actuals.fc_flag = 1 then first_value(actuals.act_cr_screening_to_qscreening_last_4_weeks) over(order by actuals.act_cr_screening_to_qscreening_last_4_weeks is null, actuals.date desc)
						else actuals.act_cr_screening_to_qscreening_last_4_weeks end
						as fc_cr_screening_to_qscreening,

						case 
						when actuals.fc_flag = 1 then first_value(actuals.act_cr_qscreening_to_trial_last_4_weeks) over(order by actuals.act_cr_qscreening_to_trial_last_4_weeks is null, actuals.date desc)
						else actuals.act_cr_qscreening_to_trial_last_4_weeks end
						as fc_cr_qscreening_to_trial,

						case 
						when actuals.fc_flag = 1 then first_value(actuals.act_lead_to_lead_tickets_called_ratio_last_4_weeks) over(order by actuals.act_lead_to_lead_tickets_called_ratio_last_4_weeks is null, actuals.date desc)
						else actuals.act_lead_to_lead_tickets_called_ratio_last_4_weeks end
						as fc_lead_to_lead_tickets_called_ratio,

						case 
						when actuals.fc_flag = 1 then first_value(actuals.act_lead_tickets_called_reached_share_last_4_weeks) over(order by actuals.act_lead_tickets_called_reached_share_last_4_weeks is null, actuals.date desc)
						else actuals.act_lead_tickets_called_reached_share_last_4_weeks end
						as fc_lead_tickets_called_reached_share,

						case 
						when actuals.fc_flag = 1 then first_value(actuals.act_cr_lead_ticket_called_reached_to_scheduled_appointment_last_4_weeks) over(order by actuals.act_cr_lead_ticket_called_reached_to_scheduled_appointment_last_4_weeks is null, actuals.date desc)
						else actuals.act_cr_lead_ticket_called_reached_to_scheduled_appointment_last_4_weeks end
						as fc_cr_lead_ticket_called_reached_to_scheduled_appointment,

						case 
						when actuals.fc_flag = 1 then first_value(actuals.act_call_attempt_ratio_last_4_weeks) over(order by actuals.act_call_attempt_ratio_last_4_weeks is null, actuals.date desc)
						else actuals.act_call_attempt_ratio_last_4_weeks end
						as fc_call_attempt_ratio,

						case 
						when actuals.fc_flag = 1 then first_value(actuals.act_call_reached_share_last_4_weeks) over(order by actuals.act_call_reached_share_last_4_weeks is null, actuals.date desc)
						else actuals.act_call_reached_share_last_4_weeks end
						as fc_call_reached_share,

						case 
						when actuals.fc_flag = 1 then first_value(actuals.act_inbound_call_ratio_last_4_weeks) over(order by actuals.act_inbound_call_ratio_last_4_weeks is null, actuals.date desc)
						else actuals.act_inbound_call_ratio_last_4_weeks end
						as fc_inbound_call_ratio,

						case 
						when actuals.fc_flag = 1 then first_value(actuals.act_sickness_rate_last_4_weeks) over(order by actuals.act_sickness_rate_last_4_weeks is null, actuals.date desc)
						else actuals.act_sickness_rate_last_4_weeks end
						as fc_sickness_rate,

					from actuals


					left join

					(select
						fim.date,
						cast(fim.planned_marketing_spend as float64) as fc_marketing_costs

					from `sonova-marketing`.data.fact_planned_marketing_spend fim

					) mfc

					on actuals.date = mfc.date


					left join

					(select
						fip.reporting_week,
						fip.aht_s_call_attempt as fc_aht_s_call_attempt,
						fip.aht_s_appm_call as fc_aht_s_appointment_call,
						fip.aht_s_reminder_call as fc_aht_s_reminder_call,
						fip.aht_s_noshow_call as fc_aht_s_noshow_call,
						fip.aht_s_followup_call as fc_aht_s_followup_call,
						fip.aht_s_second_followup_call as fc_aht_s_second_followup_call,
						fip.aht_s_inbound_call as fc_aht_s_inbound_call,
						fip.training_quality_h / 7 as fc_training_quality_h, 	--convert to daily hours (based on a 7-day work week)
						fip.meeting_h / 7 as fc_meeting_h, 						--convert to daily hours (based on a 7-day work week)
						fip.other_shrinkage_h / 7 as fc_other_shrinkage_h, 		--convert to daily hours (based on a 7-day work week)
						fip.occupancy as fc_occupancy,
						fip.holiday_rate as fc_holiday_rate,
						fip.fte_h_week as act_fte_h_week,
						fip.fte_h_week / 7 as act_fte_h_day, 					--convert to daily fte (based on a 7-day work week)

					from `sonova-marketing`.data.fact_planned_ccc_productivity_shrinkage fip

					) prd

					on actuals.reporting_week = prd.reporting_week

				) unif

			) unif2

		) unif3

	) unif4