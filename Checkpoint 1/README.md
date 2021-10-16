# Checkpoint 1



1. What is the correlation factor between the number of years in the force of an officer and the number of unsustained allegations that a police officer has? 

```
SELECT id, appointed_date,resignation_date, unsustained_count
from data_officer
```

We are getting the correlation factor and we are using the current date of the active officer - the appointed date and then we made the unsustainted_count 0 counts null. We did this amongst all active officers:
```
SELECT CORR(diff, usc) FROM (
SELECT (CURRENT_DATE::date - appointed_date::date)/365 AS DIFF, COALESCE(unsustained_count, 0) as usc
FROM data_officer
WHERE active = 'Yes') as temp;

```



2. What is the average number of times a frequently accused police officer changes the police department unit to another district?

```
CREATE TABLE unit_changes3 AS
    SELECT count(data_officerhistory.unit_id) as unit_count, data_officerhistory.officer_id,
            sum(data_officer.unsustained_count) as sum_allegations
    FROM data_officerhistory, data_officer
    WHERE data_officerhistory.id = data_officer.id
    GROUP BY officer_id;
```

Then execute this code to find number of allegations on the 75 percentile:
```
SELECT DISTINCT
    PERCENTILE_Cont(0.75) WITHIN GROUP (ORDER BY sum_allegations)
FROM unit_changes3;
```

Then execute this code to find the average of transitions from unit for the 75 percentile with more allegations:
```
SELECT
    AVG(unit_count) FROM unit_changes3 WHERE sum_allegations >= 16;
```

Then execute this code to find number of allegations on the 50 percentile:
```
SELECT DISTINCT
    PERCENTILE_Cont(0.5) WITHIN GROUP (ORDER BY sum_allegations)
FROM unit_changes3;
```

Then execute this code to find the average of transitions from unit between 50 and 75 percentile with more allegations:
```
SELECT
    AVG(unit_count) FROM unit_changes3 WHERE sum_allegations >= 6 and sum_allegations < 16;
```


3. Which are the top 5 neighborhoods with the most number of civilian complaints that have been unsustained? What percentage of unsustained complaints have each one from the total?

```
SELECT complaints.area_id, complaintcount
FROM (SELECT sum(d.count) as capitacount, area_id
FROM data_racepopulation as d
group by d.area_id) capita
JOIN (SELECT area_id, count(data_officerallegation.allegation_id) as complaintcount
FROM data_officerallegation
JOIN (SELECT d1.allegation_id, d1.area_id
FROM data_allegation_areas AS d1
JOIN data_allegation AS d2
ON d2.crid = d1.allegation_id
where d2.is_officer_complaint = 'FALSE') civilian_complaints
ON civilian_complaints.allegation_id = data_officerallegation.allegation_id
WHERE final_finding = 'UN' OR final_finding = 'EX' OR final_finding = 'NS'
GROUP BY area_id) complaints
ON capita.area_id = complaints.area_id
ORDER BY (complaintcount*1000/capitacount) DESC LIMIT 5
```

4. What is the correlation between the number of unsustained allegations against a police officer and the number of awards that officer has? 

```
SELECT CORR(SUB.T,SUB.A)
FROM
(SELECT count(tracking_no) as t, count(allegation_id) as a, data_officerallegation.officer_id
FROM data_officerallegation
JOIN data_award
ON data_award.officer_id = data_officerallegation.officer_id
WHERE final_finding = 'UN' OR final_finding = 'EX' OR final_finding = 'NS'
GROUP BY data_officerallegation.officer_id) AS SUB
```

5. What is the average percentage of civilian and officer unsustained allegations an officer has?

```
SELECT
    SUM(CASE WHEN final_finding<>'SU' and is_officer_complaint = false THEN 1 ELSE 0 END):: float/SUM(CASE WHEN is_officer_complaint= true or is_officer_complaint= false THEN 1 ELSE 0 END)  percent_unsustained_civilian,
    SUM(CASE WHEN final_finding<>'SU' and is_officer_complaint = true THEN 1 ELSE 0 END):: float/SUM(CASE WHEN is_officer_complaint= true or is_officer_complaint= false THEN 1 ELSE 0 END)  percent_unsustained_officer
FROM data_allegation JOIN data_officerallegation d on data_allegation.crid = d.allegation_id;
```










