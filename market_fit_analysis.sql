select
	unif.* except(marketing_spend, num_clicks),

	case
	when sum(unif.num_leads) over(partition by unif.date, unif.marketing_channel, unif.campaign, unif.lead_form_type, unif.lead_form_mode) is not null
	then unif.marketing_spend * unif.device_type_share_day
	when sum(unif.num_leads) over(partition by unif.reporting_month, unif.marketing_channel, unif.campaign, unif.lead_form_type, unif.lead_form_mode) is not null
	then unif.marketing_spend * unif.device_type_share_month
	when sum(unif.num_leads) over(partition by unif.date, unif.marketing_channel, unif.campaign, unif.lead_form_type, unif.lead_form_mode) is null
	then unif.marketing_spend * unif.device_type_share_raw end
	as marketing_spend,

	case
	when sum(unif.num_leads) over(partition by unif.date, unif.marketing_channel, unif.campaign, unif.lead_form_type, unif.lead_form_mode) is not null
	then unif.num_clicks * unif.device_type_share_day
	when sum(unif.num_leads) over(partition by unif.reporting_month, unif.marketing_channel, unif.campaign, unif.lead_form_type, unif.lead_form_mode) is not null
	then unif.num_clicks * unif.device_type_share_month	
	when sum(unif.num_leads) over(partition by unif.date, unif.marketing_channel, unif.campaign, unif.lead_form_type, unif.lead_form_mode) is null
	then unif.num_clicks * unif.device_type_share_raw end
	as num_clicks,

from

	(select
		unif1.*,

		safe_divide(sum(unif1.num_leads) over(partition by unif1.date, unif1.marketing_channel, unif1.campaign, unif1.lead_form_type, unif1.lead_form_mode, unif1.device_type),
		sum(unif1.num_leads) over(partition by unif1.date, unif1.marketing_channel, unif1.campaign, unif1.lead_form_type, unif1.lead_form_mode))
		as device_type_share_day,

		safe_divide(sum(unif1.num_leads) over(partition by unif1.reporting_month, unif1.marketing_channel, unif1.campaign, unif1.lead_form_type, unif1.lead_form_mode,unif1.device_type),
		sum(unif1.num_leads) over(partition by unif1.reporting_month, unif1.marketing_channel, unif1.campaign, unif1.lead_form_type, unif1.lead_form_mode))
		as device_type_share_month,

		case when unif1.marketing_spend is not null then safe_divide(1, count(distinct unif1.device_type) over (partition by (case when unif1.marketing_spend is not null then 1 else 0 end), unif1.date, unif1.marketing_channel, unif1.campaign, unif1.lead_form_type, unif1.lead_form_mode)) end as device_type_share_raw,
	
	from

		(select
			
			coalesce(ld.date, mkt.date, pw.date, app.date, scr.date, sal.date) as date,
			concat(cast(format_date('%G', coalesce(ld.date, mkt.date, pw.date, app.date, scr.date, sal.date)) as string), '-W', cast(format_date('%V', coalesce(ld.date, mkt.date, pw.date, app.date, scr.date, sal.date)) as string)) as reporting_week,
			cast(format_date('%Y-%m', coalesce(ld.date, mkt.date, pw.date, app.date, scr.date, sal.date)) as string) as reporting_month,
			coalesce(ld.marketing_channel, mkt.marketing_channel, pw.marketing_channel, app.marketing_channel, scr.marketing_channel, sal.marketing_channel) as marketing_channel,
			coalesce(ld.campaign, mkt.campaign, pw.campaign, app.campaign, scr.campaign, sal.campaign) as campaign,
			coalesce(ld.lead_form_type, mkt.lead_form_type, pw.lead_form_type, app.lead_form_type, scr.lead_form_type, sal.lead_form_type) as lead_form_type,
			coalesce(ld.lead_form_mode, mkt.lead_form_mode, pw.lead_form_mode, app.lead_form_mode, scr.lead_form_mode, sal.lead_form_mode) as lead_form_mode,
			coalesce(ld.device_type, mkt.device_type, pw.device_type, app.device_type, scr.device_type, sal.device_type) as device_type,
			mkt.marketing_spend,
			pw.num_sessions,
			mkt.num_clicks,
			ld.num_leads,
			ld.num_qualified_leads,
			app.num_scheduled_appointments,
			scr.num_screenings,
			scr.num_qualified_screenings,
			sal.num_invoices,
			sal.total_net_sales_hea_in_eur,
			sal.total_net_sales_in_eur,

		from

			(select
				flm.lead_date as date,
				flm.marketing_channel,
	            flm.campaign,
				flm.lead_form_type,
				flm.lead_form_mode,
				flm.device_type,
				count(distinct flm.lead_id) as num_leads,
				count(distinct case when flm.qualified_flag = 1 then flm.lead_id end) as num_qualified_leads,

			from `sonova-marketing`.`data_uk`.fact_lead_main flm

			group by 1,2,3,4,5,6

			) ld


			full join

			(select *

			from

				(select
					dma.date,
					dma.marketing_channel,
					dma.campaign,
					dma.lead_form_type,
					dma.lead_form_mode,
					sum(dma.marketing_costs_in_eur) as marketing_spend,
					sum(dma.num_clicks) as num_clicks,

				from `sonova-marketing`.`data_uk`.fact_daily_marketing_activity dma

				group by 1,2,3,4,5

				)

				cross join

				unnest(array(select distinct flm.device_type from `sonova-marketing`.`data_uk`.fact_lead_main flm)) as device_type

			) mkt

			on ld.date = mkt.date
			and ld.marketing_channel = mkt.marketing_channel
			and ld.campaign = mkt.campaign
			and ld.lead_form_type = mkt.lead_form_type
			and ld.lead_form_mode = mkt.lead_form_mode
			and ld.device_type = mkt.device_type


			full join

			(select * from

				(select
					fsm.date,
					fsm.marketing_channel,
		            fsm.campaign,
					lp.lead_form_type,
					lp.lead_form_mode,

					case
					when fsm.device_type = 'Phablet' then 'Tablet'
					when fsm.device_type = 'Smartphone' then 'Mobile'
					else fsm.device_type end
					as device_type,

					count(distinct fsm.session_id) as num_sessions

				from `sonova-marketing`.`data_uk`.fact_session_main fsm
				left join `sonova-marketing`.data_uk.dim_landing_pages lp
				on fsm.landing_page = lp.landing_page

				group by 1,2,3,4,5,6

				) pw1

			where pw1.device_type in ('Desktop', 'Tablet', 'Mobile')

			) pw

			on ld.date = pw.date
			and ld.marketing_channel = pw.marketing_channel
			and ld.campaign = pw.campaign
			and ld.lead_form_type = pw.lead_form_type
			and ld.lead_form_mode = pw.lead_form_mode
			and ld.device_type = pw.device_type


			full join

			(select
				ftm.created_date as date,
				ftm.marketing_channel,
	            ftm.campaign,
				ftm.lead_form_type,
				ftm.lead_form_mode,
				ftm.device_type,
				count(distinct ftm.ccc_ticket_id) as num_scheduled_appointments,

			from `sonova-marketing`.`data_uk`.fact_ticket_main ftm

			where 
				ftm.appointment_type_raw in ('fha', 'scr')
				and ftm.appointment_date is not null

			group by 1,2,3,4,5,6

			) app

			on ld.date = app.date
			and ld.marketing_channel = app.marketing_channel
			and ld.campaign = app.campaign
			and ld.lead_form_type = app.lead_form_type
			and ld.lead_form_mode = app.lead_form_mode
			and ld.device_type = app.device_type


			full join

			(select
				date(dc.first_lead_date) as date,
				dc.acquisition_marketing_channel as marketing_channel,
				dc.acquisition_marketing_campaign as campaign,
				dc.acquisition_form_type as lead_form_type,
				dc.acquisition_form_mode as lead_form_mode,
				dc.acquisition_device_type as device_type,
				count(distinct fsm.screening_id) as num_screenings,
				count(distinct case when fsm.qualified_flag = 1 then fsm.screening_id else null end) as num_qualified_screenings,

			from `sonova-marketing`.`data_uk`.fact_screening_main fsm
			right join `sonova-marketing`.`data_uk`.dim_customer dc
			on dc.customer_id = fsm.customer_id

			where fsm.post_lead_flag = 1

			group by 1,2,3,4,5,6

			) scr

			on ld.date = scr.date
			and ld.marketing_channel = scr.marketing_channel
			and ld.campaign = scr.campaign
			and ld.lead_form_type = scr.lead_form_type
			and ld.lead_form_mode = scr.lead_form_mode
			and ld.device_type = scr.device_type


			full join

			(select
				date(dc.first_lead_date) as date,
				dc.acquisition_marketing_channel as marketing_channel,
				dc.acquisition_marketing_campaign as campaign,
				dc.acquisition_form_type as lead_form_type,
				dc.acquisition_form_mode as lead_form_mode,
				dc.acquisition_device_type as device_type,
				count(distinct case when fsl.hea_quantity_invoiced > 0 then fsl.invoice_id else null end) as num_invoices,
				sum(fsl.total_net_sales_in_eur) as total_net_sales_in_eur,
				sum(fsl.total_net_sales_hea_in_eur) as total_net_sales_hea_in_eur,

			from `sonova-marketing`.`data_uk`.fact_sales_main fsl
			right join `sonova-marketing`.`data_uk`.dim_customer dc
			on dc.customer_id = fsl.customer_id

			where fsl.post_lead_flag = 1

			group by 1,2,3,4,5,6

			) sal

			on ld.date = sal.date
			and ld.marketing_channel = sal.marketing_channel
			and ld.campaign = sal.campaign
			and ld.lead_form_type = sal.lead_form_type
			and ld.lead_form_mode = sal.lead_form_mode
			and ld.device_type = sal.device_type

		) unif1

	) unif
