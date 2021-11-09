create temp table salary_table_3 as
select avg_salary, alleg
from (select avg(salary) as avg_salary, officer_id
from data_salary
group by officer_id) sub1

join (
select count(allegation_id) as alleg, officer_id
from data_officerallegation
where (final_finding = 'UN' OR final_finding = 'EX' OR final_finding = 'NS')
group by officer_id) sub2
on sub1.officer_id = sub2.officer_id;

select s.range as avg_salary, count(alleg) as allegations
from (
      select alleg,
         case when avg_salary > 30000 and avg_salary <= 50000 then '30-50'
         when avg_salary > 50000  and avg_salary <= 70000 then '50-70'
         when avg_salary > 70000 and avg_salary <= 90000 then '70-90'
         when avg_salary > 90000 and avg_salary <= 110000 then '90-110'
         when avg_salary > 110000 and avg_salary <= 130000 then '110-130'
         when avg_salary > 130000 and avg_salary <= 150000 then '130-150'
         else '150-above' end as range
     from salary_table_3) s
group by s.range