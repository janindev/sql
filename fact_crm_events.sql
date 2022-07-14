select
	fin.activity_date,
	fin.cohort_date,						--classic cohort date: date the e-mail type was first send to the user 
	
   	case 
   	when fin.activity_date >= fin.cohort_date_alt then fin.cohort_date_alt
    when fin.activity_date < fin.cohort_date_alt then fin.cohort_date
    else null end 
    as cohort_date_alt,						--alternative cohort date: date the e-mail type was first send to the user in this campaign's run

	fin.crm_category,
	fin.event_type,
	fin.email_type_rank,
	fin.email_type,	
	fin.email_type_concat,
	fin.email_event_rank,
	fin.email_event,
	fin.email_event_concat,
	fin.user_email,
	fin.user_first_name,
	fin.user_last_name,
	fin.clang_uuid,
	fin.user_event_rank_asc,
	fin.user_event_rank_desc,

from

	(select
		fin3.*,
		nth_value(fin3.next_cohort_date, 1 ignore nulls) over (partition by fin3.user_email, fin3.email_type order by fin3.user_event_rank_asc asc) as cohort_date_alt,

	from

		(select
			fin2.*,

			case
			when fin2.email_event_rank = 1 and lead(fin2.activity_date, 1) over(partition by (case when fin2.email_event_rank = 1 then 1 else 0 end), fin2.user_email, fin2.email_type order by fin2.activity_date asc) is not null
			then lead(fin2.activity_date, 1) over(partition by (case when fin2.email_event_rank = 1 then 1 else 0 end), fin2.user_email, fin2.email_type order by fin2.activity_date asc)
			when fin2.email_event_rank = 1 and lead(fin2.activity_date, 1) over(partition by (case when fin2.email_event_rank = 1 then 1 else 0 end), fin2.user_email, fin2.email_type order by fin2.activity_date asc) is null
			then fin2.activity_date end
			as next_cohort_date,

			row_number() over(partition by fin2.user_email order by fin2.activity_date asc, fin2.email_type_rank asc, fin2.email_event_rank asc, fin2.clang_uuid asc) as user_event_rank_asc,
			row_number() over(partition by fin2.user_email order by fin2.activity_date desc, fin2.email_type_rank desc, fin2.email_event_rank desc, fin2.clang_uuid desc) as user_event_rank_desc,				

		from

			(select
				fin1.* except(user_first_name, user_last_name),
				min(case when fin1.email_event_rank = 1 then fin1.activity_date end) over(partition by fin1.user_email, fin1.email_type) as cohort_date,	--classic cohort reporting: e-mail type first sent to user date
				concat(fin1.email_type_rank, ' - ', fin1.email_type) as email_type_concat,
				concat(fin1.email_event_rank, ' - ', fin1.email_event) as email_event_concat,	
				max(fin1.user_first_name) over(partition by fin1.user_key) as user_first_name,
				max(fin1.user_last_name) over(partition by fin1.user_key) as user_last_name,

			from

				(select
					cast(base.event_date as date) as activity_date,
					base.event_timestamp,
					base.crm_category,
					base.event_type,
					base.email_type,

					case
					when regexp_extract(right(base.email_type, 1), r"([0-9])") = right(base.email_type, 1) then cast(right(base.email_type, 1) as int64) 
					else 1 end
					as email_type_rank,

					case 
					when base.event_name = 'email_bounced' then 'E-Mail Bounced'
					when base.event_name = 'email_sent' then 'E-Mail Sent'
					when base.event_name = 'email_opened' then 'E-Mail Opened'
					when base.event_name = 'email_clicked' then 'E-Mail Clicked' end
					as email_event,

					case 
					when base.event_name = 'email_bounced' then 0
					when base.event_name = 'email_sent' then 1
					when base.event_name = 'email_opened' then 2
					when base.event_name = 'email_clicked' then 3 end
					as email_event_rank,
					
					base.user_email,
					base.clang_uuid,
					base.user_key,
					dat1.user_properties,
					user_props.key as user_props_key,
					case when user_props.key = 'firstName' then user_props.value end as user_first_name,
					case when user_props.key = 'lastName' then user_props.value end as user_last_name,

				from

					(select distinct 
						base1.*,
						dense_rank() over(order by base1.user_email) as user_key,

					from

						(select 
							gte.event_date,
							gte.event_timestamp,
							gte.event_name,
							gte.event_category as crm_category,
							gte.event_type,
							case when event_details.key = 'emailName' then event_details.value end as email_type,
							user_props.value as user_email,
							gte.user_id as clang_uuid,

						from `sonova-marketing`.gtm.events gte		
						cross join unnest(gte.user_properties) as user_props
						cross join unnest(gte.event_params) as event_details

						where 
							gte.event_category in ('CRM Email', 'Onboarding Email')
							and user_props.key = 'emailAddress'

						) base1

					) base

					left join

					(select 
						*, 
						case when event_details.key = 'emailName' then event_details.value end as email_type,			
						case when user_props.key = 'emailAddress' then user_props.value end as user_email,

					from `sonova-marketing`.gtm.events gte		
					cross join unnest(gte.user_properties) as user_props
					cross join unnest(gte.event_params) as event_details
					
					where gte.event_category in ('CRM Email', 'Onboarding Email')

					) dat1

					on base.user_email = dat1.user_email
					and base.event_date = dat1.event_date
					and base.event_timestamp = dat1.event_timestamp
					and base.event_type = dat1.event_type
					and base.event_name = dat1.event_name 

					cross join unnest(dat1.user_properties) as user_props		

				) fin1
				
			) fin2

			where fin2.user_props_key = 'emailAddress'	

		) fin3

	) fin