-- drop table DA;

CREATE TEMP TABLE DA AS
SELECT years_in_force, avg(total_allegations)::float4 as score

FROM

(SELECT officer_id, sum(case when end_date is not null then (DATE_PART('year', end_date::date) - DATE_PART('year', effective_date::date))
    when o.active = 'No' and end_date is null then (DATE_PART('year', effective_date::date) - DATE_PART('year', effective_date::date))
    when o.active = 'Yes' and end_date is null then (DATE_PART('year', date('01/01/2021')) - DATE_PART('year', effective_date::date)) end ) years_in_force

FROM data_officerhistory
JOIN data_officer o
on data_officerhistory.officer_id = o.id
group by officer_id) YEARS

JOIN
(SELECT count(allegation_id) as total_allegations, officer_id
FROM data_allegation a
JOIN data_officerallegation d
on a.crid = d.allegation_id
WHERE final_finding = 'UN' OR final_finding = 'EX' OR final_finding = 'NS' AND a.is_officer_complaint = FALSE
GROUP BY officer_id) allegations

ON YEARS.officer_id = allegations.officer_id
where years_in_force != 0
GROUP BY years_in_force;


select t.range as score_range, sum(score) as allegations
from (
      select score,
         case when years_in_force > 0 and years_in_force <= 10 then '1-10'
         when years_in_force > 10  and years_in_force <= 20 then '10-20'
         when years_in_force > 20 and years_in_force <= 30 then '20-30'
         when years_in_force > 30 and years_in_force <= 40 then '30-40'
         when years_in_force > 40 and years_in_force <= 50 then '40-50'
         else '50-60' end as range
     from DA) t
group by t.range

