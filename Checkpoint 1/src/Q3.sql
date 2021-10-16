SELECT sum(sub.normalised_count)
FROM(
SELECT capita.area_id, complaintcount*1000/capitacount as normalised_count
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
ORDER BY (complaintcount*1000/capitacount) DESC) as sub --6655

SELECT sub.normalised_count/66.55 --this is the total count of the column derived in the above query
FROM(
SELECT capita.area_id, complaintcount*1000/capitacount as normalised_count
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
ORDER BY (complaintcount*1000/capitacount) DESC) as sub
