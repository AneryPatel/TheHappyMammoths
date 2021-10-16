SELECT CORR(SUB.T,SUB.A)
FROM
(SELECT count(tracking_no) as t, count(allegation_id) as a, data_officerallegation.officer_id
FROM data_officerallegation
JOIN data_award
ON data_award.officer_id = data_officerallegation.officer_id
WHERE final_finding = 'UN' OR final_finding = 'EX' OR final_finding = 'NS'
GROUP BY data_officerallegation.officer_id) AS SUB
