-- question 1 we are bringing up the appointed_date, the resignation_date, and the unsustained, count for our correlation factor
SELECT id, appointed_date,resignation_date, unsustained_count
from data_officer

-- we are getting the corr factor and we are using current date of the active officer - the appointed date and then we made the unsustainted_count 0 counts null. We did this amongst all active officers
select corr(diff, usc) from (
SELECT (CURRENT_DATE::date - appointed_date::date)/365 AS DIFF, COALESCE(unsustained_count, 0) as usc
FROM data_officer
WHERE active = 'Yes') as temp
