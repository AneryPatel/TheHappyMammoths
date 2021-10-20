create table heatmap3 as
    SELECT
        data_area.polygon, data_area.median_income, data_area.name,
        (case when sum(data_racepopulation.count)>0 then sum(complaintcount)/sum(data_racepopulation.count)::float end) unsustained_percapita
    FROM data_officer
    JOIN data_area
      ON data_area.id = data_officer.id
    JOIN data_racepopulation
      ON data_racepopulation.id = data_area.id
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
    ON data_racepopulation.area_id = complaints.area_id
    group by polygon, median_income, name;