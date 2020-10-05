

select count(1) > 0 from fact_conversion_funnel
where da = (CURRENT_DATE -1);
-- check of

select
   user_id ,
  device_id ,
  initial_referring_domain ,
  region ,
  platform ,
  event_date,
  COUNT(1)
from fact_conversion_funnel
where da = (CURRENT_DATE -1)
GROUP BY 1,2,3,4,5
HAVING count(1) > 0;
-- should have 0 rows