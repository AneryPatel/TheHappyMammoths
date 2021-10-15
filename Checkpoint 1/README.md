# Checkpoint 1



1. What is the correlation factor between the rank of an officer and the number of unsustained allegations that a police officer has ( normalised by number of years in the force of an officer)? 



2. What is the average number of times a frequently accused police officer changes the police department unit to another district?

```
CREATE TABLE unit_changes2 AS
    SELECT count(data_officerhistory.unit_id) as unit_count, data_officerhistory.officer_id as off_id,
            sum(data_officer.allegation_count) as sum_allegations
    FROM data_officerhistory, data_officer
    WHERE data_officerhistory.id = data_officer.id
    GROUP BY officer_id;

/* Find number of allegations on the 90 percentile */
SELECT DISTINCT
    PERCENTILE_Cont(0.9) WITHIN GROUP (ORDER BY sum_allegations)
FROM unit_changes2;

/* Find average of transitions from unit for the 90 percentile */
SELECT
    AVG(unit_count) FROM unit_changes2 WHERE sum_allegations > 64;
    
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










