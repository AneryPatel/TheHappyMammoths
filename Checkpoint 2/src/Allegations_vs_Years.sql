CREATE TABLE years_versus_allegations AS
SELECT years_in_force, avg(total_allegations)::float4 as average_number_of_allegations

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