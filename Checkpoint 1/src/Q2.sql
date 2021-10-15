
CREATE TABLE unit_changes2 AS
    SELECT count(data_officerhistory.unit_id) as unit_count, data_officerhistory.officer_id,
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