select 

dso.* except(monday_open_time, tuesday_open_time, wednesday_open_time, thursday_open_time, friday_open_time, saturday_open_time, active_status),

concat(dso.address, ' ', dso.postal_code, ' ', dso.city) as full_address,
safe_cast(substring(split(dso.monday_open_time, ', ')[safe_offset(0)],1,5) as time format 'hh24:mi') as monday_open_time_1_start,
safe_cast(substring(split(dso.monday_open_time, ', ')[safe_offset(0)],7,5) as time format 'hh24:mi') as monday_open_time_1_end,
safe_cast(substring(split(dso.monday_open_time, ', ')[safe_offset(1)],1,5) as time format 'hh24:mi') as monday_open_time_2_start,
safe_cast(substring(split(dso.monday_open_time, ', ')[safe_offset(1)],7,5) as time format 'hh24:mi') as monday_open_time_2_end,
safe_cast(substring(split(dso.tuesday_open_time, ', ')[safe_offset(0)],1,5) as time format 'hh24:mi') as tuesday_open_time_1_start,
safe_cast(substring(split(dso.tuesday_open_time, ', ')[safe_offset(0)],7,5) as time format 'hh24:mi') as tuesday_open_time_1_end,
safe_cast(substring(split(dso.tuesday_open_time, ', ')[safe_offset(1)],1,5) as time format 'hh24:mi') as tuesday_open_time_2_start,
safe_cast(substring(split(dso.tuesday_open_time, ', ')[safe_offset(1)],7,5) as time format 'hh24:mi') as tuesday_open_time_2_end,
safe_cast(substring(split(dso.wednesday_open_time, ', ')[safe_offset(0)],1,5) as time format 'hh24:mi') as wednesday_open_time_1_start,
safe_cast(substring(split(dso.wednesday_open_time, ', ')[safe_offset(0)],7,5) as time format 'hh24:mi') as wednesday_open_time_1_end,
safe_cast(substring(split(dso.wednesday_open_time, ', ')[safe_offset(1)],1,5) as time format 'hh24:mi') as wednesday_open_time_2_start,
safe_cast(substring(split(dso.wednesday_open_time, ', ')[safe_offset(1)],7,5) as time format 'hh24:mi') as wednesday_open_time_2_end,
safe_cast(substring(split(dso.thursday_open_time, ', ')[safe_offset(0)],1,5) as time format 'hh24:mi') as thursday_open_time_1_start,
safe_cast(substring(split(dso.thursday_open_time, ', ')[safe_offset(0)],7,5) as time format 'hh24:mi') as thursday_open_time_1_end,
safe_cast(substring(split(dso.thursday_open_time, ', ')[safe_offset(1)],1,5) as time format 'hh24:mi') as thursday_open_time_2_start,
safe_cast(substring(split(dso.thursday_open_time, ', ')[safe_offset(1)],7,5) as time format 'hh24:mi') as thursday_open_time_2_end,
safe_cast(substring(split(dso.friday_open_time, ', ')[safe_offset(0)],1,5) as time format 'hh24:mi') as friday_open_time_1_start,
safe_cast(substring(split(dso.friday_open_time, ', ')[safe_offset(0)],7,5) as time format 'hh24:mi') as friday_open_time_1_end,
safe_cast(substring(split(dso.friday_open_time, ', ')[safe_offset(1)],1,5) as time format 'hh24:mi') as friday_open_time_2_start,
safe_cast(substring(split(dso.friday_open_time, ', ')[safe_offset(1)],7,5) as time format 'hh24:mi') as friday_open_time_2_end,
safe_cast(substring(split(dso.saturday_open_time, ', ')[safe_offset(0)],1,5) as time format 'hh24:mi') as saturday_open_time_1_start,
safe_cast(substring(split(dso.saturday_open_time, ', ')[safe_offset(0)],7,5) as time format 'hh24:mi') as saturday_open_time_1_end,
safe_cast(substring(split(dso.saturday_open_time, ', ')[safe_offset(1)],1,5) as time format 'hh24:mi') as saturday_open_time_2_start,
safe_cast(substring(split(dso.saturday_open_time, ', ')[safe_offset(1)],7,5) as time format 'hh24:mi') as saturday_open_time_2_end,
case when lower(dso.active_status) like '%inaktiv%' then 'Inactive' else 'Active' end as active_status,
case when dso.district_lead = 'Hauffe, Tobias' and dso.shop_id not in ('226', '240', '916') then 1 else 0 end as quix_ite_flag,
case when dps.shop_id is not null then 1 else 0 end as pilot_shop_flag,

case 
when dps.excluded_shop_flag = 1 then 1 
when dps.shop_id is not null and dso.district_lead = 'Forgber, Pia' then 1
else 0 end 
as excluded_shop_flag,

from `sonova-marketing.import.dim_shop_overview` dso 
left join `sonova-marketing.data.dim_pilot_shop` dps
on dso.shop_id = dps.shop_id