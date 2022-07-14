select 	
	fin.* 

	except(
	appointment_trigger_event_date, 
	oht_trigger_event_date, 
	next_oht_trigger_event_date, 
	next_appointment_trigger_event_date, 
	post_crm_appointment_flag, 
	post_crm_oht_flag),

	case 
	when fin.post_crm_appointment_flag = 1 and row_number() over(partition by fin.user_email, fin.post_crm_appointment_flag order by fin.activity_date desc, fin.user_event_date_rank_desc) = 1 
	then 1 
	else 0 end 
	as post_crm_appointment_flag,				--assign appointment to last date of a triggering user event in case multiple events may have triggered the appointment

	case 
	when fin.post_crm_appointment_flag = 1 and row_number() over(partition by fin.user_email, fin.post_crm_appointment_flag order by fin.activity_date desc) = 1 
	then date_diff(fin.last_appointment_scheduled_date, fin.cohort_date, day) 
	else null end
	as days_sent_to_appointment_scheduled,		--cohort date = email type sent date

	case 
	when fin.post_crm_oht_flag = 1 and row_number() over(partition by fin.user_email, fin.post_crm_oht_flag order by fin.activity_date desc, fin.user_event_date_rank_desc) = 1 
	then 1 
	else 0 end 
	as post_crm_oht_flag,						--assign oht test taken to last date of a triggering user event in case multiple events may have triggered the oht					

from

	(select distinct
		crm.*,
		lead.lead_date,

		case 
		when lead.lead_date > crm.cohort_date then null 
		when lead.lead_date = crm.cohort_date then 0
		else date_diff(crm.cohort_date, lead.lead_date, day) end
		as lead_age_days,		
		
		tic.customer_id,
		app.last_appointment_scheduled_date,

		case
		when (crm.appointment_trigger_event_date is not null and crm.next_appointment_trigger_event_date is not null and app.last_appointment_scheduled_date between crm.appointment_trigger_event_date and crm.next_appointment_trigger_event_date)
		or (crm.next_appointment_trigger_event_date is null and crm.next_appointment_trigger_event_date is null and app.last_appointment_scheduled_date >= crm.appointment_trigger_event_date)
		and date_diff(app.last_appointment_scheduled_date, crm.appointment_trigger_event_date, day) <= 30
		then 1
		else 0 end 
		as post_crm_appointment_flag,

		oht.oht_date,
		
		case 
		when (crm.oht_trigger_event_date is not null and crm.next_oht_trigger_event_date is not null and oht.oht_date between crm.oht_trigger_event_date and crm.next_oht_trigger_event_date)
	 	or (crm.next_oht_trigger_event_date is null and crm.next_oht_trigger_event_date is null and oht.oht_date >= crm.oht_trigger_event_date)
	 	and date_diff(oht.oht_date, crm.oht_trigger_event_date, day) <= 30
	 	then 1
	 	else 0 end 
		as post_crm_oht_flag,

	from

		(select 
			crm1.*,
			case when crm1.cohort_date is null then 0 else 1 end as exclude_data_flag, 		--flag to exclude data in cohort reporting where the cohort date could not be retrieved due to missing email_sent data in the gtm.events table
			lead(crm1.oht_trigger_event_date, 1) over(partition by crm1.user_email order by crm1.user_event_rank_asc asc) as next_oht_trigger_event_date,
			lead(crm1.appointment_trigger_event_date, 1) over(partition by crm1.user_email order by crm1.user_event_rank_asc asc) as next_appointment_trigger_event_date,		
			case when crm1.email_event_rank = 2 then date_diff(crm1.activity_date, crm1.cohort_date, day) end as days_sent_to_open,
			case when crm1.email_event_rank = 3 then date_diff(crm1.activity_date, crm1.cohort_date, day) end as days_sent_to_clicked,

		from 

			(select
				fce.* except(cohort_date),
				fce.cohort_date_alt as cohort_date,
				concat(cast(format_date('%Y', fce.activity_date) as string), '-W', cast(format_date('%W', fce.activity_date) as string)) as activity_reporting_week,
				cast(format_date('%Y-%m', fce.activity_date) as string) as activity_reporting_month,
				concat(cast(format_date('%Y', fce.cohort_date_alt) as string), '-W', cast(format_date('%W', fce.cohort_date_alt) as string)) as cohort_reporting_week,
				cast(format_date('%Y-%m', fce.cohort_date_alt) as string) as cohort_reporting_month,
				lead(fce.activity_date, 1) over(partition by fce.user_email order by fce.user_event_rank_asc asc) as next_email_event_date,
				row_number() over(partition by fce.user_email, fce.activity_date order by fce.email_type_rank desc, fce.email_event_rank desc, fce.clang_uuid desc) as user_event_date_rank_desc, 	--ranking for user events on the same activity date
				
				case 
				when fce.email_type in ('Onboarding V3 | Qualified | no HEA | Reminder 1',  'CRM N.3') 
				or fce.event_type = 'Retarget CRM' 
				then fce.activity_date end 
				as oht_trigger_event_date,
				
				case 
				when fce.event_type in ('Biweekly CRM', 'No-Show Cancellation CRM', 'Reactivation CRM', 'Retarget CRM') 
				then fce.activity_date end 
				as appointment_trigger_event_date,

			from `sonova-marketing`.data.fact_crm_events fce

			where 
				fce.event_type in ('Biweekly CRM', 'No-Show Cancellation CRM', 'Reactivation CRM', 'Retarget CRM')
				or fce.email_type ='Onboarding V3 | Qualified | no HEA | Reminder 1'

			) crm1

		) crm


		left join 

		(select distinct
			flm.uuid,
			flm.email,
			date(flm.lead_date) as lead_date,

		from `sonova-marketing`.data.fact_lead_main flm

		where
			flm.uuid is not null
			and flm.uuid <> ''

		) lead

		on crm.clang_uuid = lead.uuid


		left join

		(select distinct
			tic1.email,
			tic1.customer_id

		from

			(select distinct
				ftm.email,
				ftm.customer_id,
				case when ftm.email is not null and ftm.customer_id is not null then row_number() over(partition by (case when ftm.email is not null and ftm.customer_id is not null then 1 else 0 end), ftm.email order by ftm.customer_id asc) end as customer_id_rank_asc,

			from `sonova-marketing`.data.fact_ticket_main ftm

			where 
				ftm.test_flag = 0
				and ftm.customer_id is not null 
				and ftm.email is not null
				and ftm.contact_result not in ('other_ticket_not_valid', 'duplicate')

			) tic1

		where tic1.customer_id_rank_asc = 1 --in case of multiple customer ids take the first customer id assigned

		) tic

		on crm.user_email = tic.email


		left join

		(select distinct
			fam.customer_id,
			case when fam.appointment_type = 'Sales' then date(max(fam.appointment_created_at) over(partition by (case when fam.appointment_type = 'Sales' then 1 else 0 end), fam.customer_id)) end as last_appointment_scheduled_date,

		from `sonova-marketing`.data.fact_appointment_main fam 

		where fam.appointment_type = 'Sales'

		) app 

		on tic.customer_id = app.customer_id


		left join

		(select distinct
			htr.email,
			htr.date_created as oht_date,

		from `sonova-marketing`.`data`.fact_oht_results htr 

		where htr.oht_test_rank_desc = 1	

		) oht 

		on crm.user_email = oht.email

	) fin