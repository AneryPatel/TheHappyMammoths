create temp table awards as
select award_count, alleg
from (select count(tracking_no) as award_count, officer_id
from data_award
where (current_status != 'Deleted' and current_status != 'Denied')
group by officer_id) sub1

join (
select count(allegation_id) as alleg, officer_id
from data_officerallegation
where (final_finding = 'UN' OR final_finding = 'EX' OR final_finding = 'NS')
group by officer_id) sub2
on sub1.officer_id = sub2.officer_id;


select t.range as award_count, count(alleg) as allegations
from (
      select alleg,
         case when award_count > 0 and award_count <= 20 then '0-20'
         when award_count > 20  and award_count <= 40 then '20-40'
         when award_count > 40 and award_count <= 60 then '40-60'
         when award_count > 60 and award_count <= 80 then '60-80'
         when award_count > 80 and award_count <= 100 then '80-100'
         when award_count > 100 and award_count <= 120 then '100-120'
         else '120-above' end as range
     from awards) t
group by t.range