SELECT data_area.name, data_area.area_type,
           sum(data_officer.unsustained_count) as unsustained_allegations_sum, EXTRACT(YEAR from data_officer.appointed_date) as appointed_year
    FROM data_officer, data_area
    WHERE data_officer.id = data_area.id
    GROUP BY data_area.name, data_area.area_type,data_officer.appointed_date;
