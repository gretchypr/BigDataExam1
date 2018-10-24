# You sql follows
-- table escuela schema: (region STRING, district STRING, city STRING, id INT, name STRING, level STRING, series_num INT)
-- table student schema: (school_region STRING, school_district STRING, school_id INT, school_name STRING, school_level STRING, gender
--                        char(1), id INT)
-- (1) Find total number of students per region and city:
select region, city, count(*)
from escuela as e, student as s
where e.id = s.school_id
group by region, city;
-- (2) Find the total number of schools per city and level
select city, level, count(*)
from escuela
group by city, level;
-- (3) Find the total number of female students from Ponce that go a 'Superior' level school.
select count(*)
from escuela as e, student as s
where e.id = s.school_id and e.city='Ponce' and e.level='Superior' and s.gender='F';
-- (4) Find the total number of male students per region, district and city
select region, district, city, count(*)
from escuela as e, student as s
where e.id = s.school_id and s.gender='M'
group by region, district, city;
