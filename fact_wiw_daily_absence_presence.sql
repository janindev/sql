select distinct
	fin.date,
	fin.start_date,
	fin.end_date,
	fin.employee,
	fin.employee_id,
	fin.position,
	fin.day_productivity_hours,
	fin.start_time,
	fin.end_time,
	fin.type,
	fin.absence_type,
	fin.absence_total_period_days,
	fin.absence_total_period_hours,
	fin.status,
	fin.load_date,

from

	--wiw data
	(select 
		unif.date,
		unif.start_date,
		unif.end_date,
		unif.employee,
		unif.employee_id,
		pos.position,
		unif.day_productivity_hours,
		unif.start_time,
		unif.end_time,
		unif.type,
		unif.absence_type,
		unif.absence_total_period_days,
		unif.absence_total_period_hours,
		unif.status,
		unif.load_date,
		
	from

		(select 
			unif1.*,
			concat(cast(format_date('%Y-%m', date(unif1.date)) as string), '-', case when extract(day from unif1.date) < 15 then '01' else '15' end) as month_half,

		from

			(select * --unified data

			from

				--absence data
				(select 	
					fin3.date,
					fin3.start_date,
					fin3.end_date,
					fin3.employee,
					fin3.employee_id,
					'Absence' as type,
					fin3.absence_type,
					fin3.day_productivity_hours,
					cast(null as time) as start_time, 			--no reliable entries
					cast(null as time) as end_time,				--no reliable entries
					fin3.total_period_days as absence_total_period_days,
					fin3.total_period_hours as absence_total_period_hours,
					fin3.status,
					date(fin3.load_date) as load_date,
					fin3.max_load_date

				from

					(select 
						fin2.*,
						concat(cast(format_date('%Y-%m', date(fin2.date)) as string), '-', case when extract(day from fin2.date) < 15 then '01' else '15' end) as month_half,
					    max(fin2.load_date) over(partition by fin2.date) as max_load_date

					from

						(select
							fin1.load_date,
							fin1.abs_id,
							fin1.abs_date_list as date,
							fin1.start_date,
							fin1.end_date,
							fin1.employee,
							fin1.employee_id,
							safe_divide(fin1.total_period_hours, count(fin1.start_date) over(partition by fin1.abs_id)) as day_productivity_hours,	--count the days of the absence with this id
							fin1.total_period_hours,
							count(fin1.start_date) over(partition by fin1.abs_id) as total_period_days,	
							fin1.absence_type,
							fin1.status,	

						from

							(select * except (abs_date_array)

							from

								(select
						     		row_number() over() as abs_id,															--give each absence an id													
						     		wto.load_date,
									wto.start_date,
									wto.end_date,
									generate_date_array(wto.start_date, wto.end_date, interval 1 day) as abs_date_array,	--generate a date array entry (one cell) containing the dates of the absence period
									wto.employee,
									wto.employee_id,
									wto.paid_hours as total_period_hours,
									wto.type as absence_type,
									wto.status,

								from

									(select
										wto.load_date,
										wto.start_date,
										wto.start_time,
										wto.end_date,
										wto.end_time,
										wto.employee,
										wto.employee_id,
										wto.paid_hours,
										wto.type,
										wto.status

									from `sonova-marketing`.when_i_work.time_off_request wto) wto

									) abs	

								cross join unnest(abs.abs_date_array) as abs_date_list							--unnest the date array to create a date entry for each of the dates of the absence period
							  
							) fin1  

						) fin2

					) fin3

				where 
					fin3.status = 'Approved'
					and fin3.load_date = fin3.max_load_date

				) abs

				union all

				select prsfin.*

				from

					(select
						prs.date,
						prs.start_date,
						prs.end_date,
						prs.employee,
						prs.employee_id,
						'Presence' as type,
						cast(null as string) as absence_type,			--not relevant
						prs.day_productivity_hours,
						prs.start_time,
						prs.end_time,
						cast(null as float64) as total_period_days,		--not relevant
						cast(null as float64) as total_period_hours,	--not relevant
						prs.status,
						date(prs.load_date) as load_date,	
						prs.max_load_date,

					from

						(select
							prs1.*,
							prs1.start_date as date,
							concat(cast(format_date('%Y-%m', date(prs1.start_date)) as string), '-', case when extract(day from prs1.start_date) < 15 then '01' else '15' end) as month_half,
							max(prs1.load_date) over(partition by prs1.start_date) as max_load_date,
						
						from		

							(select
								wsc.load_date,
								date(wsc.start) as start_date,
								extract(time from wsc.start) as start_time,
								date(wsc.end) as end_date,
								extract(time from wsc.end) as end_time,
								wsc.employee,
								wsc.employee_id,
								wsc.position,
								wsc.total_hours as day_productivity_hours,
								wsc.status,

							from `sonova-marketing`.when_i_work.schedule wsc

							) prs1

						) prs

					where 
						prs.status = 'Published'
						and prs.load_date = prs.max_load_date

					) prsfin

			) unif1

		) unif


		left join

		--employee position data w/ half-monthly updates
		(select distinct
			pos2.month_half,
			pos2.employee,
			pos2.highest_position_no,
			
			case 
			when pos2.highest_position_no = 1 then 'Head of CCC'
			when pos2.highest_position_no = 2 then 'Teamleader'
			when pos2.highest_position_no = 3 then 'Tooling'
			when pos2.highest_position_no = 4 then 'CCC Agent'
			when pos2.highest_position_no is null then null end 
			as position,

		from

			(select 
				pos1.month_half,
				pos1.month,
				pos1.year,
				pos1.employee,
				coalesce(pos1.highest_position_no_month_half, pos1.highest_position_no_month, pos1.highest_position_no_year, pos1.highest_position_no_alltime) as highest_position_no

			from

				(select
					base.month_half,
					base.month,
					base.year,
					base.employee,
					dat.position,
					dat.highest_position_no_month_half,
					min(dat.position_no) over(partition by base.employee, base.month) as highest_position_no_month,
					min(dat.position_no) over(partition by base.employee, base.year) as highest_position_no_year,
					min(dat.position_no) over(partition by base.employee) as highest_position_no_alltime,

				from

					--base table with distinct month halfs and employees
					(select distinct 
						base2.month_half,
						base2.month,
						base2.year,
						base2.employee

					from

						(select *

						from

							(select
								dat.month_half,
								dat.month,
								dat.year,
								wso.employee_array
									
								from
								
									(select distinct
										concat(cast(format_date('%Y-%m', date(predat.date)) as string), '-', case when extract(day from predat.date) < 15 then '01' else '15' end) as month_half,
										cast(format_date('%Y-%m', date(predat.date)) as string) as month,					
										cast(extract(year from predat.date) as string) as year,

									from
									
										(select *
									
										from unnest(generate_date_array('2019-08-01', date_add(current_date(), interval 180 day))) as date
									
										) predat
									
									) dat
									
									
									left join
									
									
									(select  
										concat(cast(format_date('%Y-%m', date(wso.date)) as string), '-', case when extract(day from wso.date) < 15 then '01' else '15' end) as month_half,
										
										(select 
											(array(select distinct wso.employee

										from `sonova-marketing`.when_i_work.schedule wso)

										)) as employee_array,
									
									from `sonova-marketing`.when_i_work.schedule wso
									
									) wso
										
									on dat.month_half = wso.month_half

								) base1

							cross join unnest(base1.employee_array) as employee

						) base2

					) base


					left join


					--postion data
					(select
						dat1.employee,
						dat1.position,
						dat1.position_no,
						dat1.month_half,
						min(position_no) over(partition by dat1.employee, dat1.month_half) as highest_position_no_month_half,

					from

						(select distinct 
							concat(cast(format_date('%Y-%m', date(wso.date)) as string), '-', case when extract(day from wso.date) < 15 then '01' else '15' end) as month_half,
							wso.employee, 
							wso.position, 

							case 
							when wso.position = 'Head of CCC' then 1
							when wso.position = 'Teamleader' then 2
							when wso.position = 'Tooling' then 3
							when wso.position in ('CCC Agent', 'CCC Agent (UK)') then 4 end 
							as position_no,

							date(wso.load_date) as load_date,
							max(date(wso.load_date)) over(partition by date(wso.date)) as max_load_date,
					
						from `sonova-marketing`.when_i_work.schedule wso

						) dat1

					where dat1.load_date = dat1.max_load_date

					) dat
					
					on base.month_half = dat.month_half 
					and base.employee = dat.employee

				) pos1

			) pos2

		) pos	

		on unif.employee = pos.employee 
		and unif.month_half = pos.month_half

	where pos.position = 'CCC Agent'

	) fin

left join`sonova-marketing`.data.dim_agent_main ag
fin.employee = ag.full_name

where ag.country = 'DE'