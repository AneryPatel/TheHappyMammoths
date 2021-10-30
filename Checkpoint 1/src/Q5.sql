SELECT
    SUM(CASE WHEN final_finding<>'SU' and is_officer_complaint = false THEN 1 ELSE 0 END):: float/SUM(CASE WHEN is_officer_complaint= true
    or is_officer_complaint= false THEN 1 ELSE 0 END)  percent_unsustained_civilian,
    SUM(CASE WHEN final_finding<>'SU' and is_officer_complaint = true THEN 1 ELSE 0 END):: float/SUM(CASE WHEN is_officer_complaint= true
    or is_officer_complaint= false THEN 1 ELSE 0 END)  percent_unsustained_officer,
    SUM(CASE WHEN final_finding='SU' and is_officer_complaint = false THEN 1 ELSE 0 END):: float/SUM(CASE WHEN is_officer_complaint= true
    or is_officer_complaint= false THEN 1 ELSE 0 END)  percent_sustained_civilian,
    SUM(CASE WHEN final_finding='SU' and is_officer_complaint = true THEN 1 ELSE 0 END):: float/SUM(CASE WHEN is_officer_complaint= true
    or is_officer_complaint= false THEN 1 ELSE 0 END)  percent_sustained_officer

FROM data_allegation JOIN data_officerallegation d on data_allegation.crid = d.allegation_id;
