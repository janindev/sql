select distinct
	tic.customer_key,
	tic.customer_id,
	tic.zendesk_ticket_id,
	tic.ticket_type,
	tic.lead_flag,
	tic.qualified_lead_flag,
	tic.phone_ticket_flag,
	tic.pilot_flag,
	tic.duplicate_flag,
	tic.valid_phone_number_flag,
	tic.ticket_date,
	tic.ticket_time,
	concat(cast(format_date('%G', tic.ticket_date) as string), '-W', cast(format_date('%V', tic.ticket_date) as string)) as ticket_reporting_week,
	cast(format_date('%Y-%m', tic.ticket_date) as string) as ticket_reporting_month,
	tic.business_hours_flag,
	tic.ticket_status,
	tic.ticket_closed_date,
	tic.appointment_flag,
	tic.appointment_90min_flag,
	tic.appointment_30min_flag,
	tic.post_no_show_appointment_flag,
	case when unif.first_time_to_call is null then 1 else tic.phone_number_ticket_rank_asc end as phone_number_ticket_rank_asc,
	unif.call_id,
	unif.call_type,
	unif.call_date,
	concat(cast(format_date('%G', unif.call_date) as string), '-W', cast(format_date('%V', unif.call_date) as string)) as call_reporting_week,
	cast(format_date('%Y-%m', unif.call_date) as string) as call_reporting_month,
	unif.call_start_time,
	unif.call_end_time,
	unif.call_rank_asc,
	unif.outbound_call_rank_asc,
	unif.total_calls,
	unif.total_outbound_calls,
	unif.post_no_show_call_flag,
	unif.first_time_to_call,				--calculated for outbound calls only, i.e. initiated by ccc
	unif.first_time_to_call_sec,			--calculated for outbound calls only, i.e. initiated by ccc
	unif.first_time_to_call_days,			--calculated for outbound calls only, i.e. initiated by ccc
	unif.first_time_to_interaction,			--calculated for outbound calls only, i.e. initiated by ccc
	unif.first_time_to_interaction_sec,		--calculated for outbound calls only, i.e. initiated by ccc
	unif.first_time_to_interaction_days,	--calculated for outbound calls only, i.e. initiated by ccc
	unif.call_duration,
	unif.in_call_duration,
	unif.interaction_flag,
	unif.agent_name,
	unif.phone_number_aspect_aircall,
	tic.phone_number_zendesk,
	tic.phone_number_leadform,
	tic.email_address_zendesk,
	unif.load_date,

	case
	when tic.ticket_created_at > max(unif.latest_load_date) over() then 1							--no call data for tickets created after the latest load
	when tic.ticket_created_at < min(date(unif.earliest_call_data_date)) over() then 1				--no call data for tickets created before the first call data entry
	when tic.ticket_date between '2020-10-27' and '2020-11-06' then 1								--no call data from 2020-10-26 (12:28:44) until 2020-11-06
	else 0 end
	as outside_call_data_range_flag

from

	(select
		tic1.*,
		dense_rank() over(partition by tic1.phone_number_zendesk order by tic1.zendesk_ticket_id asc) as phone_number_ticket_rank_asc,

	from

		(select distinct
			ftm.customer_key,
			ftm.customer_id,
			ftm.zendesk_ticket_id,
			ftm.ticket_type,
			ftm.lead_flag,
			case when ftm.qualified_flag = 1 then 1 else 0 end as qualified_lead_flag,
			ftm.phone_ticket_flag,
			ftm.pilot_flag,
			case when ftm.contact_reason_raw like '%Datensatz doppelt' or ftm.contact_result = 'other_ticket_not_valid' then 1 else 0 end as duplicate_flag,
			case when ftm.contact_reason_raw like '%Falsche Telefonnummer' or ftm.contact_result_specific = 'Wrong phone number' then 0 else 1 end as valid_phone_number_flag,
			ftm.created_at as ticket_created_at,
			date(ftm.created_at) as ticket_date,
			extract(time from ftm.created_at) as ticket_time,
			ftm.created_during_business_hours as business_hours_flag,
			ftm.ticket_status,
			case when ftm.ticket_status in ('Closed', 'Solved') then date(ftm.last_updated_at) end as ticket_closed_date,
			case when ftm.lead_flag = 1 and ftm.contact_reason in ('Hearing Test Only', 'Hearing Test & Consultation') and ftm.appointment_date is not null then 1 else 0 end as appointment_flag,
			case when ftm.lead_flag = 1 and ftm.contact_reason = 'Hearing Test & Consultation' and ftm.appointment_date is not null then 1 else 0 end as appointment_90min_flag,
			case when ftm.lead_flag = 1 and ftm.contact_reason = 'Hearing Test Only' and ftm.appointment_date is not null then 1 else 0 end as appointment_30min_flag,
			ftm.phone_number as phone_number_zendesk,
			cast(regexp_extract(ftm.description, r"Telefon:\ ([+0-9]+)") as string) as phone_number_leadform,
			ftm.email as email_address_zendesk,

			max(case
			when ftu.updated_at > ftu.no_show_updated_at and ftu.contact_reason in ('Hearing Test Only', 'Hearing Test & Consultation') and ftu.appointment_date is not null 
			then 1 else 0 end) over(partition by ftu.zendesk_ticket_id)
			as post_no_show_appointment_flag,

		from `sonova-marketing`.data.fact_ticket_main ftm
		left join `sonova-marketing`.data.fact_ticket_update ftu
		on ftm.zendesk_ticket_id = ftu.zendesk_ticket_id

		where ftm.test_flag = 0

		) tic1

	) tic


	left join

	(select *

	from

		(select
			unif6.*,
			round(safe_divide(safe_divide(safe_divide(unif6.first_time_to_call_sec, 60), 60), 24), 1) as first_time_to_call_days,
			round(safe_divide(safe_divide(safe_divide(unif6.first_time_to_interaction_sec, 60), 60), 24), 1) as first_time_to_interaction_days,
			max(unif6.call_rank_asc) over(partition by unif6.zendesk_ticket_id) as total_calls,
			max(unif6.outbound_call_rank_asc) over(partition by unif6.zendesk_ticket_id) as total_outbound_calls,

		from

			(select
				unif5.*,
				row_number() over(partition by unif5.zendesk_ticket_id, unif5.phone_number_ticket_rank_asc order by unif5.call_timestamp asc) as call_rank_asc,
				case when unif5.call_type = 'Outbound' then row_number() over(partition by (case when unif5.call_type = 'Outbound' then 1 else 0 end), unif5.zendesk_ticket_id, unif5.phone_number_ticket_rank_asc order by unif5.call_timestamp asc) end as outbound_call_rank_asc,				
				case when unif5.first_time_to_call < unif5.ticket_created_at then null else datetime_diff(unif5.first_time_to_call, unif5.ticket_created_at, second) end as first_time_to_call_sec,
				case when unif5.first_time_to_interaction < unif5.ticket_created_at then null else datetime_diff(unif5.first_time_to_interaction, unif5.ticket_created_at, second) end as first_time_to_interaction_sec,

			from

				(select
					unif4.*,
					case when unif4.call_type = 'Outbound' then min(unif4.call_timestamp) over(partition by (case when unif4.call_type = 'Outbound' then 1 else 0 end), unif4.phone_number_zendesk, unif4.phone_number_ticket_rank_asc) end as first_time_to_call,
					case when unif4.call_type = 'Outbound' then min(case when unif4.interaction_flag = 1 then unif4.call_timestamp end) over(partition by (case when unif4.call_type = 'Outbound' then 1 else 0 end), unif4.phone_number_zendesk, unif4.phone_number_ticket_rank_asc) end as first_time_to_interaction,

					case
					when unif4.no_show_updated_at is not null and unif4.call_timestamp > unif4.no_show_updated_at then 1
					when unif4.no_show_updated_at is null then 0
					else 0 end
					as post_no_show_call_flag,

				from

					(select
						unif3.*,

						case 
						when
						unif3.ticket_created_at = unif3.previous_ticket_created_at 
						and unif3.call_timestamp between datetime_sub(unif3.ticket_created_at, interval 60 minute) and unif3.next_ticket_created_at then 1 --assign calls to first ticket of the phone number
						when 
						unif3.ticket_created_at > unif3.previous_ticket_created_at and unif3.ticket_created_at < unif3.last_ticket_created_at
						and unif3.call_timestamp between unif3.ticket_created_at and unif3.next_ticket_created_at then 1	--assign calls to inbetween tickets of that phone number
						when unif3.ticket_created_at = unif3.last_ticket_created_at 
						and unif3.call_timestamp > unif3.ticket_created_at then 1 	--assign calls to the last ticket of that phone number
						else 0 end
						as ticket_assignment_flag
					
					from

						(select
							unif2.*,
							max(unif2.prep_next_ticket_created_at) over(partition by unif2.phone_number_zendesk, unif2.phone_number_ticket_rank_asc) as next_ticket_created_at,
							max(unif2.prep_previous_ticket_created_at) over(partition by unif2.phone_number_zendesk, unif2.phone_number_ticket_rank_asc) as previous_ticket_created_at,
							max(unif2.ticket_created_at) over(partition by unif2.phone_number_zendesk) as last_ticket_created_at,

						from

							(select
								unif1.*,

								case
								when lead(unif1.ticket_created_at, 1) over(partition by unif1.phone_number_zendesk order by unif1.ticket_created_at) is null then unif1.ticket_created_at
								else lead(unif1.ticket_created_at, 1) over(partition by unif1.phone_number_zendesk order by unif1.ticket_created_at) end
								as prep_next_ticket_created_at,

								case
								when lag(unif1.ticket_created_at, 1) over(partition by unif1.phone_number_zendesk order by unif1.ticket_created_at) is null then unif1.ticket_created_at
								else lag(unif1.ticket_created_at, 1) over(partition by unif1.phone_number_zendesk order by unif1.ticket_created_at) end
								as prep_previous_ticket_created_at,

							from

								(select *

								from

									--join aspect/aircall data on phone number from zendesk customer profile
									(select
										zd1.*,
										dense_rank() over(partition by zd1.phone_number_zendesk order by zd1.zendesk_ticket_id asc) as phone_number_ticket_rank_asc,

									from

										(select distinct
											ftm.phone_number as phone_number_zendesk,
											cast(regexp_extract(ftm.description, r"Telefon:\ ([+0-9]+)") as string) as phone_number_leadform,
											ftm.zendesk_ticket_id,
											case when ftm.contact_reason_raw like '%Datensatz doppelt' or ftm.contact_result = 'other_ticket_not_valid' then 1 else 0 end as duplicate_flag,
											case when ftm.contact_reason_raw like '%Falsche Telefonnummer' or ftm.contact_result_specific = 'Wrong phone number' then 0 else 1 end as valid_phone_number_flag,
											case when ftm.contact_reason_raw like '%Fake/SPAM/Test' or ftm.contact_result_specific in ('Fake / Spam', 'Fake / Spam / Test', 'Test') then 1 else 0 end as spam_flag,
											ftm.created_at as ticket_created_at,
											date(ftm.created_at) as ticket_date,
											extract(time from ftm.created_at) as ticket_time,
											ftm.no_show_updated_at,

										from `sonova-marketing`.`data`.fact_ticket_main ftm

										where ftm.test_flag = 0

										) zd1

									where 
										zd1.duplicate_flag = 0			--filter out duplicate tickets
										and zd1.phone_number_zendesk <> 'anonymous'
										and zd1.spam_flag = 0

									) zd


									left join

									(select
										fcm.call_id,
										fcm.call_timestamp,
										fcm.call_date,
										fcm.call_start_time,
										fcm.call_end_time,
										fcm.call_type,
										fcm.call_total_time as call_duration,
										ifnull(fcm.call_hold_time, 0) + ifnull(fcm.call_talk_time, 0) as in_call_duration,
										fcm.interaction_flag,
										
										case 
										when fcm.source = 'aspect' then fcm.customer_phone_no 
										when fcm.source = 'aircall' and regexp_extract(fcm.customer_phone_no, r"(?:\+[0-9]*)(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*(?: )*([0-9]*)*") is not null
										then 
										concat(
										regexp_extract(fcm.customer_phone_no, r"(\+[0-9]*)(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*"),
										regexp_extract(fcm.customer_phone_no, r"(?:\+[0-9]*)(?: )*([0-9]*)*(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*"),
										regexp_extract(fcm.customer_phone_no, r"(?:\+[0-9]*)(?: )*(?:[0-9]*)*(?: )*([0-9]*)*(?: )*(?:[0-9]*)*"),
										regexp_extract(fcm.customer_phone_no, r"(?:\+[0-9]*)(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*(?: )*([0-9]*)*")
										)
										when fcm.source = 'aircall' and regexp_extract(fcm.customer_phone_no, r"(?:\+[0-9]*)(?: )*(?:[0-9]*)*(?: )*([0-9]*)*(?: )*(?:[0-9]*)*") is not null
										then 
										concat(
										regexp_extract(fcm.customer_phone_no, r"(\+[0-9]*)(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*"),
										regexp_extract(fcm.customer_phone_no, r"(?:\+[0-9]*)(?: )*([0-9]*)*(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*"),
										regexp_extract(fcm.customer_phone_no, r"(?:\+[0-9]*)(?: )*(?:[0-9]*)*(?: )*([0-9]*)*(?: )*(?:[0-9]*)*")
										)
										when fcm.source = 'aircall' and regexp_extract(fcm.customer_phone_no, r"(?:\+[0-9]*)(?: )*([0-9]*)*(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*") is not null
										then
										concat(
										regexp_extract(fcm.customer_phone_no, r"(\+[0-9]*)(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*"),
										regexp_extract(fcm.customer_phone_no, r"(?:\+[0-9]*)(?: )*([0-9]*)*(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*")
										)
										when fcm.source = 'aircall' and regexp_extract(fcm.customer_phone_no, r"(\+[0-9]*)(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*") is not null
										then regexp_extract(fcm.customer_phone_no, r"(\+[0-9]*)(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*(?: )*(?:[0-9]*)*")
										end
										as phone_number_aspect_aircall,
										
										fcm.agent_extension,
										fcm.agent_name,
										fcm.load_date,
										max(fcm.load_date) over() as latest_load_date,
										min(fcm.call_timestamp) over() as earliest_call_data_date

									from `sonova-marketing`.`data`.fact_call_main fcm

									) cal

									on zd.phone_number_zendesk = cal.phone_number_aspect_aircall


								union all

								select *

								from

									--join aspect/aircall data on phone number from leadform when it is not the same as the zendesk customer profile phone number
									(select zd2.*

									from

										(select
											zd1.*,
											dense_rank() over(partition by zd1.phone_number_zendesk order by zd1.zendesk_ticket_id asc) as phone_number_ticket_rank_asc,

										from

											(select distinct
												ftm.phone_number as phone_number_zendesk,
												cast(regexp_extract(ftm.description, r"Telefon:\ ([+0-9]+)") as string) as phone_number_leadform,
												ftm.zendesk_ticket_id,
												case when ftm.contact_reason_raw like '%Datensatz doppelt' or ftm.contact_result = 'other_ticket_not_valid' then 1 else 0 end as duplicate_flag,
												case when ftm.contact_reason_raw like '%Falsche Telefonnummer' or ftm.contact_result_specific = 'Wrong phone number' then 0 else 1 end as valid_phone_number_flag,
												case when ftm.contact_reason_raw like '%Fake/SPAM/Test' or ftm.contact_result_specific in ('Fake / Spam', 'Fake / Spam / Test', 'Test') then 1 else 0 end as spam_flag,
												ftm.created_at as ticket_created_at,
												date(ftm.created_at) as ticket_date,
												extract(time from ftm.created_at) as ticket_time,
												ftm.no_show_updated_at,

											from `sonova-marketing`.`data`.fact_ticket_main ftm

											where ftm.test_flag = 0

											) zd1

										where 
											zd1.duplicate_flag = 0								--filter out duplicate tickets
											and zd1.phone_number_zendesk <> 'anonymous'
											and zd1.spam_flag = 0

										) zd2

									where zd2.phone_number_leadform <> zd2.phone_number_zendesk 	--exclude double joining for tickets where the phone numbers are the same

									) zd


									left join

									(select
										fcm.call_id,
										fcm.call_timestamp,
										fcm.call_date,
										fcm.call_start_time,
										fcm.call_end_time,
										fcm.call_type,
										fcm.call_total_time as call_duration,
										ifnull(fcm.call_hold_time, 0) + ifnull(fcm.call_talk_time, 0) as in_call_duration,
										fcm.interaction_flag,
										fcm.customer_phone_no as phone_number_aspect_aircall,		--aircall automatically formats the phone number that is called, no results for aircall data will be retrieved by this match on leadform phone number
										fcm.agent_extension,
										fcm.agent_name,
										fcm.load_date,
										max(fcm.load_date) over() as latest_load_date,
										min(fcm.call_timestamp) over() as earliest_call_data_date

									from `sonova-marketing`.`data`.fact_call_main fcm

									) cal

									on zd.phone_number_leadform = cal.phone_number_aspect_aircall

								) unif1

							) unif2

						) unif3

					) unif4

					where unif4.ticket_assignment_flag = 1

				) unif5

			) unif6

		) preunif

		where
			preunif.ticket_created_at > preunif.earliest_call_data_date 	--do not consider tickets created at a date before first call data are available to avoid wrong ttc's for customers w/ multiple entries
			and preunif.ticket_created_at < preunif.latest_load_date		--do not consider tickets created after the latest load date

	) unif

	on tic.zendesk_ticket_id = unif.zendesk_ticket_id