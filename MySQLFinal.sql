----------------------------------------------------------------------
-- DROP TABLES IN REVERSE SEQUENCE OF DEPENDENCIES
----------------------------------------------------------------------
drop table if exists DM_dept_manager_tb;
drop table if exists DE_dept_emp_tb;

drop table if exists S_salaries_tb;
drop table if exists T_titles_tb;

drop table if exists E_employees_tb cascade;   	-- drop emp table and cascade to any depedencies
drop table if exists D_department_tb cascade;	-- drop dept table and cascade to any dependent tables

----------------------------------------------------------------------
-- CREATE TABLES WITH PK and FK CONSTRAINTS
----------------------------------------------------------------------
create table E_employees_tb(
	E_emp_no text primary key not null,
	E_birth_dt date not null,
	E_first_name varchar not null,
	E_last_name varchar not null,
	E_gender varchar not null,
	E_hire_dt date not null
	--constraint E_employees_tb_pk primary key (E_emp_no)
);
create table D_department_tb(
	D_dept_no text primary key not null,
	D_dept_name varchar not null
	-- constraint D_department_tb_pk primary key (D_dept_no)
);
create table S_salaries_tb(
	S_emp_no text primary key not null,
	S_salary_amt decimal not null,
	S_from_dt date not null,
	S_to_dt date not null,
	foreign key (S_emp_no) references E_employees_tb(E_emp_no)
);
create table T_titles_tb(
	T_emp_no text not null,
	T_title varchar not null,
	T_from_dt date not null,
	T_to_dt date not null,
	foreign key (T_emp_no) references E_employees_tb(E_emp_no)
);
create table DM_dept_manager_tb(
	DM_dept_no text not null,
	DM_emp_no text not null,
	DM_from_dt date not null,
	DM_to_dt date not null,
	primary key (DM_dept_no, DM_emp_no),
	foreign key (DM_dept_no) references D_department_tb(D_dept_no),
	foreign key (DM_emp_no) references E_employees_tb(E_emp_no)
);
create table DE_dept_emp_tb(
	DE_emp_no text not null,
	DE_dept_no text not null,
	DE_from_dt date not null,
	DE_to_dt date not null,
	primary key (DE_dept_no, DE_emp_no),
	foreign key (DE_dept_no) references D_department_tb(D_dept_no),
	foreign key (DE_emp_no) references E_employees_tb(E_emp_no)
);
----------------------------------------------------------------------
-- COUNT VERIFICATIONS
----------------------------------------------------------------------
select 'total rows:' 							as "counts  >>>  ",
	(select count(*) from E_employees_tb) 		as "employees",
	(select count(*) from D_department_tb) 		as "departments",
	(select count(*) from S_salaries_tb) 		as "salaries",
	(select count(*) from T_titles_tb) 			as "titles",
	(select count(*) from DM_dept_manager_tb) 	as "dept-managers",
	(select count(*) from DE_dept_emp_tb) 		as "dept-employees"
	;

----------------------------------------------------------------------
-- USE IMPORT (EXPORT) FUNCTION FROM PGADMIN TO LOAD CSV WITH HEADER
-- 1. E_employees_tb
-- 2. D_department_tb
-- 3. S_salaries_tb
-- 4. T_titles_tb
-- 5. DM_dept_manager_tb
-- 6. DE_dept_emp_tb
----------------------------------------------------------------------
	
	
-----------------------------------------------------------------------------------------------------------------
-- 1: EMPLOYEE LIST
select E_emp_no as "Emp No", E_last_name as "Last Name", E_first_name as "First Name", E_gender as "Gender" , S_salary_amt as "Salary"
from E_employees_tb E
join S_salaries_tb S on S.S_emp_no = E.E_emp_no
;
-----------------------------------------------------------------------------------------------------------------
-- 2: EMPLOYEES HIRED IN 1986
select 	E_emp_no as "Emp No", E_last_name as "Last Name", E_first_name as "First Name", E_gender as "Gender", E_hire_dt as "Hire Date"
from 	E_employees_tb E
where 	E_hire_dt between '1986-01-01' and '1986-12-31'
;
-----------------------------------------------------------------------------------------------------------------
-- 3. MANAGER LIST WITH START & END EMPLOYMENT DATES (and TITLE)
select DM_dept_no as "Dept No", D_dept_name as "Dept Name", DM_emp_no as "Manager Emp No",E_last_name as "Manager Last Name", E_first_name as "Manager First Name", E_hire_dt as "Start Date", T_to_dt "End Date", T_title as "Title"
from DM_dept_manager_tb DM
join D_department_tb D 	on D_dept_no = DM_dept_no
join E_employees_tb E 	on E_emp_no = DM_emp_no
join T_titles_tb T 		on T_emp_no = E_emp_no and T_Title = 'Manager'
;
-----------------------------------------------------------------------------------------------------------------
-- 4. EMPLOYEE LIST WITH DEPARTMENT (and START/END DATES)
--
--    NOTE: THERE ARE 2 SOL'N PRESENTED SINCE CLASS DID NOT COVER CREATING INDICES YET.
--          (a) MY FIRST APPROACH IS TO CORRELATE THE DEPT EMP NO TO USE ONLY THE LATEST DEPT WHERE EMPLOYEE WAS, WHETHER EMPLOYEE LEFT OR NOT.
--              THIS WORKS FOR SMALL LIST OF EMPLOYEES BUT RUNS FOREVER EXECUTED AGAINST THE FULL DATA. 
--              CREATING TABLE INDEXES WILL HELP ITS RUNTIME PERFORMANCE.
--          (b) AN ALTERNATIVE SOLUTION IS TO JOIN THE DEPT TABLE WHERE WE IMMEDIATELY LOOK FOR THE MAX END DATE.
--              THIS WILL BE MORE EFFICIENT AS IT BRINGS DOWN THE ROWS TO BE JOINED IMMEDIATELY, REGARDLESS OF AN AVAILABLE INDEX.
--              IT AVOIDS THE REPEATED SCANS THE FIRST SOLUTION DOES AS IT CORRELATES THE KEYS.

--     HERE IS SOLUTION (b)...
select E_emp_no as "Emp No", E_last_name as "Last Name", E_first_name as "First Name", D_Dept_name as "Dept Name", DE_from_dt as "START DATE",DE_to_dt as "END DATE"
from 	E_employees_tb E, 		DE_dept_emp_tb DE, 			D_department_tb D,
		(select DE_emp_no, max(DE_to_dt) as DE3_to_dt from DE_dept_emp_tb DE2	--where DE_emp_no in ('100001','100010','100018') --> used for checkouts
		 group by 1) DE3
where 	DE3.DE_emp_no 	= DE.DE_emp_no and DE3.DE3_to_dt = DE.DE_to_dt
and		DE.DE_emp_no 	= E_emp_no 												--and E_emp_no in ('100001','100010','100018')  --> used for checkouts
and		DE.DE_dept_no 	= D_dept_no
;
--      HERE IS SOLUTION (a)...
select E_emp_no as "Emp No", E_last_name as "Last Name", E_first_name as "First Name", D_Dept_name as "Dept Name", DE_from_dt as "START DATE", DE_to_dt as "END DATE"
from E_employees_tb E
join DE_dept_emp_tb DE 	on DE_emp_no = E_emp_no 		and E_emp_no in ('100001','100010','100018')  --> Used for checkouts; else it runs forever without an index
join D_department_tb D	on D_dept_no = DE_dept_no
where DE_to_dt = 
(select max(DE_to_dt) from DE_dept_emp_tb DE2 where DE2.DE_emp_no = DE.DE_emp_no)
;
-----------------------------------------------------------------------------------------------------------------
-- 5. "HERCULES B." LIST
select E_emp_no as "Emp No", E_last_name as "Last Name starts with B", E_first_name as "First Name is Hercules"
from E_employees_tb E
where E_first_name = 'Hercules' and E_last_name like 'B%'
;
-----------------------------------------------------------------------------------------------------------------
-- 6. EMPLOYEES IN SALES
-- select distinct count(*) 					--> used to confirm unique rows in results set
select E_emp_no as "Emp No", E_last_name as "Last Name", E_first_name as "First Name", D_dept_name as "Department"
from E_employees_tb E
join DE_dept_emp_tb DE	on DE_emp_no = E_emp_no
join D_department_tb D 	on D_dept_no = DE_dept_no and D_dept_name = 'Sales'
;
-----------------------------------------------------------------------------------------------------------------
-- 7. EMPLOYEES IN SALES AND DEVELOPMENT
-- select distinct count(*) 					--> used to confirm unique rows in results set
select E_emp_no as "Emp No", E_last_name as "Last Name", E_first_name as "First Name", D_dept_name as "Department"
from E_employees_tb E
join DE_dept_emp_tb DE	on DE_emp_no = E_emp_no
join D_department_tb D 	on D_dept_no = DE_dept_no and D_dept_name in ('Sales','Development')
;
-----------------------------------------------------------------------------------------------------------------
-- 8. SAME LAST-NAMES FROM MOST COMMON(POPULAR) TO LEAST
select E_last_name as "Employee Last Name", count(*) as "Number of Employees with same Last Name"
from E_employees_tb E
group by  E_last_name
order by 2 desc
;
