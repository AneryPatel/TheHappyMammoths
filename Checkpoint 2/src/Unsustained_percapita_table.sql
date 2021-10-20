create table heatmap1 as
    SELECT
        data_officer.unsustained_count, data_area.polygon, data_area.median_income,
        (case when sum(data_racepopulation.count)>0 then sum(data_officer.unsustained_count)/sum(data_racepopulation.count)::float end) unsustained_percapita
    FROM data_officer
    JOIN data_area
      ON data_area.id = data_officer.id
    JOIN data_racepopulation
      ON data_racepopulation.id = data_area.id
    group by polygon, unsustained_count, median_income;