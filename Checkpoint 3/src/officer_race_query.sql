create temp table race_table as
select race, alleg
from (Select race, id
from data_officer
group by id) sub1

join (
select count(allegation_id) as alleg, officer_id
from data_officerallegation
where (final_finding = 'UN' OR final_finding = 'EX' OR final_finding = 'NS')
group by officer_id) sub2
on sub1.id = sub2.officer_id;

select count(alleg), race
from race_table
group by race;
