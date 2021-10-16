
CREATE TABLE unit_changes3 AS
    SELECT count(data_officerhistory.unit_id) as unit_count, data_officerhistory.officer_id,
            sum(data_officer.unsustained_count) as sum_allegations
    FROM data_officerhistory, data_officer
    WHERE data_officerhistory.id = data_officer.id
    GROUP BY officer_id;

/* Find number of allegations on the 75 percentile */
SELECT DISTINCT
    PERCENTILE_Cont(0.75) WITHIN GROUP (ORDER BY sum_allegations)
FROM unit_changes3;

/* Find average of transitions from unit for the percentile 75 */
SELECT
    AVG(unit_count) FROM unit_changes3 WHERE sum_allegations >= 16;

/* Find number of allegations on the 50 percentile */
SELECT DISTINCT
    PERCENTILE_Cont(0.5) WITHIN GROUP (ORDER BY sum_allegations)
FROM unit_changes3;

/* Find average of transitions from unit for the percentile between 50 and 75 */
SELECT
    AVG(unit_count) FROM unit_changes3 WHERE sum_allegations >= 6 and sum_allegations < 16;
