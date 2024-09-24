
create database practice;
use practice;

create table product (productid int primary key,
product_name varchar(20)
)

CREATE TABLE sales (
    productid INT,
    date DATE,
    sales_amount INT,
    FOREIGN KEY (productid) REFERENCES product(productid)
);

insert into product values(1,'A'),(2,'B'),(3,'C');

insert into sales values(1,'2023-06-24',1000),(2,'2023-07-05',1000),(3,'2023-08-24',1500);

select * from sales
select * from product
------------------------------------------------------------------------------------------
--1)Find avg sales amount for each month

select p.productid,MONTH(s.date)as each_month,avg(s.sales_amount)as avgsales from product p join sales s on p.productid=s.productid group by MONTH(s.date),p.productid;

--2)find productid,productname with second highest sale.
with cte as(
select  p.productid,p.product_name ,s.sales_amount,
DENSE_RANK() over(order by s.sales_amount desc)as d 
from sales s join product p on 
s.productid=p.productid )
select productId,product_name,sales_amount from cte where d=2;

--3)count of sames saleamount
select sales_amount, count(sales_amount)as total_count from sales  group by sales_amount ;
select sales_amount,count(*) from sales group by sales_amount having count(*)>1;

------------------------------------------------------------------------------------------------

create table dept(deptid int primary key,
designation varchar(25)
)

create table emp(empid int primary key,
emp_name varchar(25),
manager_id int,
deptid int,
foreign key (deptid) references dept(deptid)
)

insert into dept values(1,'CEO'),(2,'CTO'),(3,'HR'),(4,'President'),(5,'Teamlead'),(6,'CheifArchitect');

insert into emp values(1,'rajeev',4,1),(2,'deekshith',4,2),(3,'arun',5,3),(4,'anand',5,4),(5,'vamshi',6,5),(6,'mahesh',4,6),(7,'sanjay',7,3),(8,'ravi',null,3); 

----------------------------------------------------------------------------------------------------------------------------------------------------------------

select * from emp
select * from dept

select e.emp_name from emp e join emp d on e.empid=d.manager_id group by e.emp_name having count(e.emp_name)>1



with cte as (select e.emp_name,count(e.emp_name)as frequent  from emp e join emp f on e.empid=f.manager_id group by e.emp_name)
select top 1 emp_name from cte order by frequent desc 

select e.empid,e.emp_name from emp e  join dept d on e.deptid=d.deptid
where e.manager_id is null;

select empid,emp_name from emp where manager_id is null


--4)find the employee who designation is ceo
create view empdept as
select e.*,d.designation from emp e join dept d on e.deptid=d.deptid;


select * from empdept where designation='CEO'


------------------------------------------------------------------

select e.empid,e.emp_name as employee_name,c.designation,d.emp_name,d.manager_id,r.designation from emp e left join emp d
on d.deptid=e.manager_id
left join dept c
on e.deptid=c.deptid
left join dept r
on d.deptid=r.deptid

----------------------------------------------------------------------
create table emp1(eid int primary key,
ename varchar(10),mid int,dptid int,foreign key(dptid) references  dept1(dptid))

create table dept1(dptid int primary key,dptname varchar(10))

insert into dept1 values(1,'it'),(2,'hr')

insert into emp1 values(1,'a',4,1),(2,'b',3,2),(3,'c',2,1),(4,'d',1,2)

select e.eid,e.ename,d.dptname as emp_dept,m.eid as mgid,m.ename as manager_name,s.dptname as manager_dept  
from emp1 e 
join emp1 m on e.mid=m.eid 
join dept1 d on e.dptid=d.dptid 
join dept1 s on m.dptid=s.dptid


----------------------------------------------
create table salary1(eid int,
salary int,
foreign key (eid) references emp1(eid)
)

insert into salary1 values (1,1000),(2,2000),(3,1000),(4,4500)

select * from salary1
-----------------------------------------------------------

select top 1 e.eid,e.ename,d.dptname,s.salary from emp1 e join salary1 s on e.eid=s.eid   join dept1 as d on e.dptid=d.dptid order by s.salary desc

select top 1 e.eid,e.ename,d.dptname from emp1 e join salary1 s on e.eid=s.eid join dept1 as d on e.dptid=d.dptid order by s.salary desc

alter view emp_salary as
(select e.*,s.salary from emp1 e join salary1 s on e.eid=s.eid)

select * from emp_salary

select salary,count(*) from emp_salary group by salary order by salary desc

--------------------------------------------------------------------------------

create table emp2(eid int,ename varchar(50),salary int)

insert into emp2 values(1,'A',2000),(2,'B',3000),(3,'C',4000),(4,'D',5000),(5,'E',3400),(6,'F',2300),(7,'G',4900)
insert into emp2 values(9,'H',3000)
select * from emp2

alter table emp2 drop column joiningdate 

--Nth highest salary
select ename,salary from(select ename,salary,RANK() over( order by salary desc)as d from emp2)as a where d=5

------------------------------------------------------------------------------------------
create proc suresh
as
begin
select * from emp2
end;

exec suresh


--with one input stored procedure

create proc suresh1
@salary int
as
begin
select * from emp2 where salary=@salary;
end;

exec suresh1 5000

--with two output

alter proc suresh3
@maxSalary int output,
@minSalary int output
as
begin
select @maxSalary=max(salary),@minSalary=min(Salary) from emp2
end;

declare @max int ,@min int
exec suresh3 @maxSalary=@max output,@minSalary=@min output
select @max as maxsalary,@min as minSalary

--------------------------------------------------------------------------

--with one input,one output

create proc suresh4
@eid int,
@salary int output
as
begin
select @salary=salary from emp2 where eid=@eid
end;

declare @sal int
exec suresh4 1,@salary=@sal output
select @sal as salary

-----------------------------------------------------------------------------------
--update value using stored procedure

create proc suresh5
@in int,
@salary int
as
begin
update  emp2
set salary=@salary
where eid=@in 
end;

exec suresh5 @in=5,@salary=3890

select * from emp2
with cte as(
select *,ROW_NUMBER() over(order by eid )as b from emp2
)
select * from cte where b%2=0

select * from emp2 where eid%2=0
use practice
alter table emp2 add joiningdate date

update emp2
set joiningdate=
case eid
when 1 then '2024-06-06'
when 2 then '2024-07-06'
when 3 then '2023-05-09'
when 4 then '2022-07-07'
when 5 then '2022-07-01'
when 6 then '2023-09-11'
when 7 then '2024-06-29'
when 8 then '2023-06-04'
end;

-------------------------------------------------------------------------------------

-----------------Functions--------------------------------------------
create function dbo.experience(@e date)
returns int
as
begin
return datediff(year,@e,getdate())
end
go
select ename,salary,dbo.experience(joiningdate)as experience from emp2;

-------------------------------------------------------------------------------

create function dbo.month(@d date)
returns int
as
begin
declare @res int;
set @res=datediff(month,@d,getdate());
return @res
end
go
select ename,salary,dbo.month(joiningdate)as months from emp2
---------------------------------------------------------------------
create function dbo.updated(@i int)
returns  table
as
return(
select ename,salary from(select ename,salary,RANK() over( order by salary desc)as d from emp2)as a where d=@i);

select * from dbo.updated(4)
select * from emp2
-----------------------------------------------------------------------
create table empaudit
(auditid int primary key identity(1,1) ,
operations varchar(10),
complete_details varchar(1000)
)
alter TRIGGER trg_Op
ON emp2
INSTEAD OF INSERT
AS
BEGIN
    -- Insert into empaudit table
    INSERT INTO empaudit (operations, complete_details)
    SELECT 
        'insert',
        CONCAT(
            'eid: ', CAST(i.eid AS NVARCHAR), ', ',
            'ename: ', i.ename, ', ',
            'salary: ', CAST(i.salary AS NVARCHAR), ', ',
            'joiningdate: ', CAST(i.joiningdate AS NVARCHAR)
        )
    FROM inserted i;
    
    -- Perform the actual insert into emp2
    INSERT INTO emp2 (eid, ename, salary, joiningdate)
    SELECT 
        i.eid,
        i.ename,
        i.salary,
        i.joiningdate
    FROM inserted i;
END;

insert into emp2 values(9,'I',6000,'2024-08-07')
select * from emp2
select * from empaudit

with cte
as
(select *,row_number() over(partition by salary order by salary)as row_num from emp2 )
delete from cte where row_num>1

insert into emp2 values (2,'B',2000)

select * from emp2
SELECT COALESCE(NULL,'PostgreSQL','SQL');
SELECT ROUND(123.4567, 2);
SELECT AVG(salary) FROM Emp2;
CREATE INDEX salary_idx ON Emp2(salary);
select * from sys.indexes 
SELECT 
    name AS index_name, 
    type_desc AS index_type, 
    is_unique 
FROM sys.indexes 
WHERE object_id = OBJECT_ID('Emp2')

select * from emp2 with(index(salary_idx))


CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(50),
    manager_id INT,
    salary DECIMAL(10, 2)
);

INSERT INTO employees (employee_id, name, manager_id, salary) VALUES
(3, 'Mila', 9, 60301),
(12, 'Antonella', NULL, 31000),
(13, 'Emery', NULL, 67084),
(1, 'Kalel', 11, 21241),
(9, 'Mikaela', NULL, 50937),
(11, 'Joziah', 6, 28485);

select e.employee_id from employees e join employees d on e.employee_id=d.manager_id where e.salary<300000 and e.manager_id != d.employee_id;

---------------------------------------------------------------------------------------------------------------------

CREATE TABLE ActivityLog (
    machine_id INT,
    process_id INT,
    activity_type VARCHAR(10),
    timestamp DECIMAL(5, 3)
);

INSERT INTO ActivityLog (machine_id, process_id, activity_type, timestamp) VALUES
(0, 0, 'start', 0.712),
(0, 0, 'end', 1.520),
(0, 1, 'start', 3.140),
(0, 1, 'end', 4.120),
(1, 0, 'start', 0.550),
(1, 0, 'end', 1.550),
(1, 1, 'start', 0.430),
(1, 1, 'end', 1.420),
(2, 0, 'start', 4.100),
(2, 0, 'end', 4.512),
(2, 1, 'start', 2.500),
(2, 1, 'end', 5.000);

select * from ActivityLog

select s.machine_id, round(avg(e.timestamp-s.timestamp),3) as processing_time 
from activitylog s 
join activitylog e 
on s.machine_id = e.machine_id
and s.process_id=e.process_id 
and s.activity_type='start' and e.activity_type='end' 
group by s.machine_id;


with b as
(select machine_id,timestamp,process_id from ActivityLog where activity_type='start'),

a as( select machine_id,timestamp,process_id from ActivityLog where activity_type='end')

select a.machine_id,round(avg(a.timestamp-b.timestamp),3)as Processing_time 
from a,b 
where a.machine_id=b.machine_id and a.process_id=b.process_id 
group by a.machine_id

---------------------------------------------------------------------------------------------------------------

select * from emp2

create table emp3(eid int,ename varchar(50),salary int)
insert into emp3 values(1,'a',100),(2,'b',100)

create function dbo.gethighestsalary2(@N int)
returns int
begin
return(
select  salary from(select salary, row_number() over( order by salary desc)as b from emp2)as a where b=@N
)
end
go
select dbo.gethighestsalary2(2) 

-----------------------------------------------------------------------------------------------
select salary, ROW_NUMBER() over(partition by salary order by salary  desc)as b from emp2
-----------------------------------------------------------------------------------------------

create proc vember(@min as int, @max as int)
as
begin
select * from emp2 where salary between @min and @max;
end;

exec vember 1000,3000

create proc vember1(@min as int,@max as int)
as
begin
update emp2 set salary=@max where eid=@min;
end;

exec vember1 1,3000

select * from emp2

create proc vember3
@i int
as
begin
DECLARE @counter INT = 1;

WHILE @counter <= 5
BEGIN
    PRINT @counter;
    SET @counter = @counter + 1;
END
end;

exec vember3 4

------------------------------------------------------

CREATE PROC EVEN
AS
BEGIN
select * from emp2 where eid%2!=0
END;


EXEC EVEN

--------------------------------------------------------------------------

ALTER PROC NthHIGHEST
@n INT
AS
BEGIN
SELECT SALARY FROM (SELECT SALARY,ROW_NUMBER() OVER(ORDER BY SALARY DESC)AS B FROM EMP2)AS A WHERE B=@n;
END;

EXEC NthHIGHEST 9


WITH CTE AS (
    SELECT * FROM (SELECT salary,ROW_NUMBER() OVER(partition by salary ORDER BY SALARY DESC)AS B FROM EMP2)AS A 
)
DELETE FROM CTE
WHERE b > 1;

select * from emp2

select @@ROWCOUNT
--------------------------------------------------------
create view hello as
select * from emp2 full join dept on emp2.eid=dept.deptid

insert into dept values(7,'CEO'),(8,'HR')

select * from hello

select distinct (designation),salary from hello  group by designation,salary order by salary desc

select coalesce(salary,0) as salary,coalesce(designation,'unknown') as designation from
( select  designation,salary,rank()over( partition by designation order by salary desc) as b from hello)as a  where b=1 


select * from emp2


select * ,Rank() over(Partition by salary order by salary desc) as b from emp2


---------------------------------------------------------------------------------------
CREATE TABLE Movies (
    id INT PRIMARY KEY,
    movie VARCHAR(50),
    description VARCHAR(255),
    rating FLOAT
);

INSERT INTO Movies (id, movie, description, rating) VALUES
(1, 'War', 'great 3D', 8.9),
(2, 'Science', 'fiction', 8.5),
(3, 'Irish', 'boring', 6.2),
(4, 'Ice Song', 'Fantasy', 8.6),
(5, 'House Card', 'Interesting', 9.1);


select * from Movies where description!='boring' and id%2!=0 order by rating desc

-------------------------------------------------------------------------------------------------------------
create table triangle
(x int,y int, z int,primary key(x,y,z))

insert into triangle
values(13,15,30),(10,20,15)


with cte as
(
select x,y,z,sum(x+y)as a,sum(x+z) as c,sum(y+z) as b from triangle group by x,y,z
)
select x,y,z from cte where ((a+b)>c)  


----------------------------------------------------------------------------------------
Movies table:
+-------------+--------------+
| movie_id    |  title       |
+-------------+--------------+
| 1           | Avengers     |
| 2           | Frozen 2     |
| 3           | Joker        |
+-------------+--------------+
Users table:
+-------------+--------------+
| user_id     |  name        |
+-------------+--------------+
| 1           | Daniel       |
| 2           | Monica       |
| 3           | Maria        |
| 4           | James        |
+-------------+--------------+
MovieRating table:
+-------------+--------------+--------------+-------------+
| movie_id    | user_id      | rating       | created_at  |
+-------------+--------------+--------------+-------------+
| 1           | 1            | 3            | 2020-01-12  |
| 1           | 2            | 4            | 2020-02-11  |
| 1           | 3            | 2            | 2020-02-12  |
| 1           | 4            | 1            | 2020-01-01  |
| 2           | 1            | 5            | 2020-02-17  | 
| 2           | 2            | 2            | 2020-02-01  | 
| 2           | 3            | 2            | 2020-03-01  |
| 3           | 1            | 3            | 2020-02-22  | 
| 3           | 2            | 4            | 2020-02-25  | 
+-------------+--------------+--------------+-------------+
Output: 
+--------------+
| results      |
+--------------+
| Daniel       |
| Frozen 2     |
+--------------+
-----------------------------------------------------------------------------

create table emp29 (eid int,ename varchar(100),deptid int,salary int)

insert into emp29 values(1,'siri',10,1000),(2,'sairam',20,2000),(3,'raju',30,3000),(3,'raju',30,3000),(1,'siri',10,1000)

select * from emp29

with cte as (select eid,ename,deptid,salary,row_number()over (partition by eid,ename,deptid,salary order by eid)as r from emp29 where deptid in (10,30))
delete   from cte where r>1

select * from emp29

select * from(
select *,dense_rank() over(order by salary ) as r from emp29
) as b where r in (2,3)

----------------------------------------------------------------------
select * from emp

select  salary from (select distinct salary ,dense_rank() over ( order by salary desc)as r from emp)as a where r=6
alter table emp add salary int

update  emp set salary= case
when empid=1 then 1000 
when empid=2 then 2000 
when empid=3 then 3000
when empid=4 then 4000 
when empid=5 then 5000
when empid=6 then 2000
when empid=7 then 1000
when empid=8 then 9000
end;


select e.empid,e.emp_name,e.salary,m.emp_name as manager,m.salary as manager_salary 
from emp e join emp m on e.empid=m.manager_id and e.salary>m.salary

--------------------------------------------------------------------------------------------------

----Print 1 to 100 prime numbers in sql-------------
declare @start int
declare @end int
declare @current int
declare @output nvarchar(max)
set @start=2
set @end=1000
set @current=@start

while @current<=@end
begin

with prime as (
select  @start as primenbr
union all
select a.primenbr+1 as value
from prime a
where a.primenbr <@end
),
b as(
select primenbr from prime a 
where not exists 
(select 1 from prime b 
where a.primenbr % b.primenbr=0 
and a.primenbr != b.primenbr)
)
)
SELECT @output = COALESCE(@output + '&', '') + CAST(primenbr AS NVARCHAR)
    FROM b
    WHERE primenbr = @current
    OPTION (MAXRECURSION 0);
set @current=@current+1
end

select @output as output

---------------------------------------------------------------------------

declare @start int
declare @end int
declare @current int
declare @output nvarchar(max)
set @start=1
set @end=10
set @current=@start

while @current<=@end
begin

with even as(
select @start as evennbr
union all
select e.evennbr+1 as value from even e where e.evennbr<@end
),
b as( select evennbr from even e where evennbr%2!=0)
SELECT @output = COALESCE(@output + '&', '') + CAST(evennbr AS NVARCHAR)
    FROM b
    WHERE evennbr = @current
    OPTION (MAXRECURSION 0);
set @current=@current+1
end

select @output as output

---------------------------------------------------------------

with prime as (
select  2 as primenbr
union all
select a.primenbr+1 as value
from prime a
where a.primenbr <10
)
select primenbr from prime a


select * from(select  *,dense_rank() over(order by salary desc) as ra
from emp2) a
where  ra in (3,5);


-------------------------------------------------------------------------------------------------

use practice

create table Mynumbers (num int)

insert into Mynumbers values (8),(8),(3),(1),(3),(1)
drop table mynumbers
select * from Mynumbers

with cte as(
select num,count(num)as count from Mynumbers  group by num 
)
select top 1 isnull(cte.num,null)as num   from cte where count=1 order by num desc


---------------------------------------------------------------------------------------------
CREATE TABLE Product1 (
    product_key INT PRIMARY KEY
);

INSERT INTO Product1 (product_key) VALUES
(5),
(6);

CREATE TABLE Customer1 (
    customer_id INT,
    product_key INT,
    FOREIGN KEY (product_key) REFERENCES Product1(product_key)
);

INSERT INTO Customer1 (customer_id, product_key) VALUES
(1, 5),
(2, 6),
(3, 5),
(3, 6),
(1, 6);

WITH cte AS (
    SELECT p.product_key, c.customer_id
    FROM Product1 p
    JOIN Customer1 c ON p.product_key = c.product_key
),
customer_product_count AS (
    SELECT customer_id, COUNT(DISTINCT product_key) AS product_count
    FROM cte
    GROUP BY customer_id
),
total_product_count AS (
    SELECT COUNT(DISTINCT product_key) AS total_count
    FROM Product1
)
SELECT cpc.customer_id
FROM customer_product_count cpc
JOIN total_product_count tpc ON cpc.product_count = tpc.total_count;

---------------------------------------------------------------------------------------------------------
09/09/2024

use practice

create table college
(
studentid int identity(581,1) primary key,
college_name varchar(20) default 'TR',
dep varchar(20),
course varchar(20)
)
create table student1
(
studentid int identity(581,1) primary key,
year_of_joining date
)

insert into college (dep,course)values('1','A0')

insert into student1 values('09-09-2019')

select * from student1
select * from college

SELECT concat(FORMAT(DATEADD(year, 0, s.year_of_joining), 'yy'),c.college_name,c.dep,c.course,s.studentid)as hallticket from student1 s join college c on s.studentid=c.studentid where year(year_of_joining)=2019

---------------------------------------------------------------------------------------------------------------------
create table register
(college_code varchar(5) default 'TR',
course_code varchar(5) default 'A',
studentid int identity(1,1) primary key,
student_name varchar(25),
dept_name varchar(5),
year_of_joining date,
scholar_type varchar(10),
hallticket_no nvarchar(50)
)
create table department
(
dept_name varchar(5),
dept_code varchar(3)
)
create table scholar
(
scholar_type varchar(10),
scholar_code int
)


insert into department values ('cse','05'),('ece','04'),('eee','02'),('ce','01'),('me','03'),('it','12')

insert into scholar values ('day',1),('lateral',5)

insert into register (student_name,dept_name,scholar_type,year_of_joining) values('Rajashekar','cse','day','09-09-2019')
select * from register

------------------------------------------------------------------------------------------------------------------------------------
-----Trigger--------------------

create trigger trg_hall
on register
after insert
as
begin

declare @stdid int;
declare @scholar varchar(10);
declare @dept varchar(5);

select @stdid = studentid,@scholar=scholar_type,@dept=dept_name from inserted;

declare @dept_code varchar(2);
select @dept_code=dept_code from department where dept_name=@dept;

declare @scholar_code int;
select @scholar_code=scholar_code from scholar where scholar_type=@scholar;



with cte as (
        select
            studentid,
            ROW_NUMBER() OVER (PARTITION BY dept_name, year(year_of_joining) ORDER BY studentid) AS row_num
        from register
    )
    UPDATE r
    SET hallticket_no = CONCAT(
        FORMAT(DATEADD(year, 0, r.year_of_joining), 'yy'),
        r.college_code,
        @scholar_code,
        r.course_code,
        @dept_code,
        RIGHT('00' + CAST(c.row_num AS
		VARCHAR(2)), 2)
    )
    from register r
    join CTE c on r.studentid = c.studentid
    where r.studentid = @stdid;

end;

insert into register (student_name,dept_name,scholar_type,year_of_joining)
values('shekar','ece','day','09-09-2019')
insert into register (student_name,dept_name,scholar_type,year_of_joining)
values('Rajashekar','cse','day','09-09-2019')
insert into register (student_name,dept_name,scholar_type,year_of_joining)
values('vinay','cse','day','09-09-2019')
insert into register (student_name,dept_name,scholar_type,year_of_joining)
values('vikas','cse','day','09-09-2019')
insert into register (student_name,dept_name,scholar_type,year_of_joining)
values('arjun','cse','day','09-09-2019')
insert into register (student_name,dept_name,scholar_type,year_of_joining)
values('arjun','ece','day','09-09-2019')
insert into register (student_name,dept_name,scholar_type,year_of_joining)
values('arjun kumar','cse','day','09-09-2020')

select * from register order by hallticket_no ;
-----------------------------------------------------------------------------------------
-----------------stored procedure-----------------------


create table student (
studentid int primary key identity,
studentname nvarchar(20),
deptid int,
yoj int,
foreign key (deptid) references dept (deptid)
 
);
 
create table dept2(
deptid int primary key,
deptname nvarchar(20)
foreign key (deptid) references college (deptid)
 
);
 
create table college (
deptid int primary key,
collegeid nvarchar(20) default 'TR1',
Universityid nvarchar(20) default 'AO'
);
 
INSERT INTO college (deptid)
VALUES
  (1),
  (2),
  (3),
  (4),
  (5);
 
INSERT INTO dept (deptid, deptname)
VALUES
  (1, 'CS'),
  (2, 'ECE'),
  (3, 'EEE'),
  (4, 'IT'),
  (5, 'CME');
 
 
  INSERT INTO student (studentname, deptid, yoj)
VALUES
  ('John Doe', 1, 2018),
  ('Jane Doe', 2, 2019),
  ('Alice Smith', 3, 2017),
  ('Bob Johnson', 4, 2018),
  ('Eve Brown', 5, 2019),
  ('Mike Davis', 1, 2020),
  ('Emma Taylor', 2, 2018),
  ('David Lee', 3, 2019),
  ('Sophia Patel', 4, 2020),
  ('Oliver White', 5, 2017);
 
 
  select * from student
  select * from dept
  select * from college
 
create view v_studentdetails
as
select s.studentid,s.studentname,s.deptid,s.yoj,d.deptname,c.collegeid,c.universityid
from student s join dept d
on s.deptid=d.deptid 
join college c on
d.deptid=c.deptid
 
select * from v_studentdetails
 
 
create procedure generate_student_ids
as
begin
	declare @rollno varchar(3)
	select cast(right(yoj,2) as nvarchar(20)) +
			cast(collegeid as nvarchar(20))  +cast(universityid as nvarchar(20))+cast(deptid as nvarchar(2)) +
			cast(format(ROW_NUMBER() OVER (PARTITION BY deptid,yoj ORDER BY studentid) ,'00')as nvarchar(13)) AS STUDENT_HALLTICKET_NO,*
	from v_studentdetails
end
 
exec generate_student_ids
 

INSERT INTO Student(student_id,student_name,dept_id) VALUES (4,'Man',1);
 
 
create function fun_generate_student_ids()
returns table
as
return(	select cast(right(yoj,2) as nvarchar(20)) +
			cast(collegeid as nvarchar(20))  +cast(universityid as nvarchar(20))+cast(deptid as nvarchar(2)) +
			cast(format(ROW_NUMBER() OVER (PARTITION BY deptid,yoj ORDER BY studentid) ,'00')as nvarchar(13)) AS STUDENT_HALLTICKET_NO,*
	from v_studentdetails);
 
select * from dbo.fun_generate_student_ids()
 
insert into student values ('sandeep',1,2019)
-----------------------------------------------------------
DECLARE @i INT = 100;
BEGIN
    PRINT RIGHT('00' + CAST(@i AS VARCHAR(2)), 2);
END;

-----------------------------------------------------------
WITH NumberSequence AS (
    SELECT 1 AS number
    UNION ALL
    SELECT number + 1
    FROM NumberSequence
    WHERE number < 135
),
AlphaNumericSequence AS (
    SELECT RIGHT('00' + CAST(number AS VARCHAR(2)), 2) AS formatted_number
    FROM NumberSequence
    WHERE number <= 99
    UNION all
    SELECT CHAR(65 + ((number - 100) / 10)) + CAST((number - 100) % 10 AS VARCHAR(1)) AS formatted_number
    FROM NumberSequence
    WHERE number >= 100 AND number <= 120 
)
SELECT formatted_number
FROM AlphaNumericSequence
OPTION (MAXRECURSION 200);



---------------------------------------------------------------------
create trigger trg_hall
on register
after insert
as
begin

declare @stdid int;
declare @scholar varchar(10);
declare @dept varchar(5);

select @stdid = studentid,@scholar=scholar_type,@dept=dept_name from inserted;

declare @dept_code varchar(2);
select @dept_code=dept_code from department where dept_name=@dept;

declare @scholar_code int;
select @scholar_code=scholar_code from scholar where scholar_type=@scholar;

DECLARE @i INT = @stdid;
Declare @result varchar(50);
set @result=(select cast(right(year_of_joining,2) as nvarchar(20)) +
			cast(college_code as nvarchar(20))  +cast(@scholar_code as varchar(10))+course_code +@dept_code,
			cast(format(ROW_NUMBER() OVER (PARTITION BY @dept_code,year_of_joining ORDER BY @i) ,'00')as nvarchar(13)) AS STUDENT_HALLTICKET_NO,*
	from register);


update  register set hallticket_no=@result
where studentid= @stdid;

end;


-----------------------------------------------------------------------

alter TRIGGER trg_hall
ON register
AFTER INSERT
AS
BEGIN
    DECLARE @stdid INT;
    DECLARE @scholar VARCHAR(10);
    DECLARE @dept VARCHAR(5);

    SELECT @stdid = studentid, @scholar = scholar_type, @dept = dept_name 
    FROM inserted;

    DECLARE @dept_code VARCHAR(2);
    SELECT @dept_code = dept_code 
    FROM department 
    WHERE dept_name = @dept;

    DECLARE @scholar_code INT;
    SELECT @scholar_code = scholar_code 
    FROM scholar 
    WHERE scholar_type = @scholar;

    WITH NumberSequence AS (
        SELECT studentid, dept_name, year_of_joining,
               ROW_NUMBER() OVER (PARTITION BY dept_name, YEAR(year_of_joining) ORDER BY studentid) AS number
        FROM register
    ),
    AlphaNumericSequence AS (
        SELECT studentid, dept_name, year_of_joining,
               RIGHT('00' + CAST(number AS VARCHAR(2)), 2) AS formatted_number
        FROM NumberSequence
        WHERE number <= 99
        UNION ALL
        SELECT studentid, dept_name, year_of_joining,
               CHAR(65 + ((number - 100) / 10)) + CAST((number - 100) % 10 AS VARCHAR(1)) AS formatted_number
        FROM NumberSequence
        WHERE number >= 100 AND number <= 120
    )
    UPDATE r
    SET hallticket_no = CONCAT(
        FORMAT(DATEADD(year, 0, r.year_of_joining), 'yy'),
        r.college_code,
        @scholar_code,
        r.course_code,
        @dept_code,
        c.formatted_number
    )
    FROM register r
    JOIN AlphaNumericSequence c ON r.studentid = c.studentid
    WHERE r.studentid = @stdid;
END;
---------------------------------------------------------------------------
select * from department
use practice

create table subjects
(
subjectid int identity(1,1) primary key,
departmentid int,
subject_code nvarchar(5),
subject_name nvarchar(25)
)

insert into subjects values(05,'cs101','Programming'),(05,'cs102','chemistry'),(05,'cs103','BEEE'),(05,'cs201','C language')
,(05,'cs202','DSA'),(05,'cs203','OS'),(04,'ec101','English'),(04,'ec102','OOPs'),(04,'ec103','EG')

---------------------------------------------------------------------------------
create table triangle(id int identity(1,1) primary key, a int,b int,c int)

select * from triangle

SELECT 
    CASE 
	    when x=y and x=z then 'equilateral triangle'
		WHEN x = y or x=z or y=z THEN 'isoceles triangle'
		when (x+y)>z then 'scalar triangle'
        ELSE 'not  triangle'
    END AS triangle_type
FROM triangle;
---------------------------------------------------------------------

create table occupation (id int identity(1,1) primary key,name varchar(10),occupation varchar(10))

INSERT INTO occupation (Name, Occupation) VALUES
('Samantha', 'Doctor'),
('Julia', 'Actor'),
('Maria', 'Actor'),
('Meera', 'Singer'),
('Ashely', 'Professor'),
('Ketty', 'Professor'),
('Christeen', 'Professor'),
('Jane', 'Actor'),
('Jenny', 'Doctor'),
('Priya', 'Singer');

with cte as(
select name,occupation,row_number() over(partition by occupation order by name asc)as r from occupation
)
select max(case when occupation='Doctor' then name end )as doctors,max(case when occupation='professor' then name end )as professors,max(case when occupation='singer' then name end)as singers,max(case when occupation='Actor' then name end)as actors from cte group by r 

insert into triangle values(10,10,10),(13,15,17),(1,2,5)
insert into triangle values(10,10,20)

----------------------------------------------------------------------------------------------
create table city
(id int identity(1,1) primary key,
name varchar(10),
country_code varchar(3),
district varchar(10),
population int
)


insert into city values ('madhapur',01,'hyd',100000),('sec',01,'hyd',200000),('jgl',07,'jgl',1000),('ameerpet',01,'hyd',2000)

with cte as (select name,population from city where population>100000)
select count(*) from cte;

with cte as
(select population,name from city where name='hyd')
select sum(population) from cte
----------------------------------------------------------------------------------------------

create table emp
(id int identity(1,1) primary key,
name varchar(10),
salary int)

insert into emp values ('a',1460),('b',2006),('c',3000),('d',2210)



select  salary = CAST(REPLACE(CAST(salary AS VARCHAR), '0', '') AS INT) from emp

select cast(ceiling(avg(cast(salary as float))- avg(CAST(REPLACE(salary, '0', '')as float))) AS INT) from emp
---------------------------------------------------------------------------------------------------------------
-- Create the employees table
CREATE TABLE employees1 (
    employee_id INT PRIMARY KEY,
    name VARCHAR(50),
    months INT,
    salary INT
);

-- Insert values into the employees table
INSERT INTO employees1 (employee_id, name, months, salary) VALUES
(122345, 'Rose', 15, 1968),
(336645, 'Angela', 1, 3443),
(456562, 'Frank', 17, 1608),
(561518, 'Patrick', 11, 1345),
(597725, 'Lisa', 7, 2330),
(741947, 'Kimberly', 16, 4372),
(835665, 'Bonnie', 6, 1771),
(886087, 'Michael', 8, 2017),
(990880, 'Todd', 5, 3396),
(101010, 'Joe', 9, 3573);

WITH cte AS (
    SELECT 
        employee_id,
        (salary * months) AS total_earnings
    FROM employees1
)
SELECT 
    MAX(total_earnings),
    COUNT(*)
FROM cte
WHERE total_earnings = (SELECT MAX(total_earnings) FROM cte);
-------------------------------------------------------------------
create table student
(id int identity(1,1) primary key,
name varchar(10),
marks int
)

insert into student values
('a', 88),('b',68),('c',99),('d',78),('e',63),('f',91)

create table grade
(grade int,
min_mark int,
max_mark int
)

insert into grade values (1,0,9),(2,10,19),(3,20,29),(4,30,39),(5,40,49),(6,50,59),(7,60,69),(8,70,79),(9,80,89),(10,90,100)

select student.name,case when grade.grade>=8 then grade.grade else null end,student.marks from student,grade where student.marks between grade.min_mark and grade.max_mark order by grade.grade desc, name;

---------------------------------------------------------------------

create table s1 (
id int,name varchar(5)
)
insert into s1 values (1,'a'),(2,'b'),(3,'c'),(4,'d')

create table friends
(id int,
friend_id int)

insert into friends values(1,4),(2,3),(3,2),(4,1)

create table packages
(id int,
salary int)

insert into packages values(1,1000),(2,2000),(3,3000),(4,4000)


with ab as
(select s1.id as stid,s1.name,friends.id as fid,p.salary
from s1 join friends 
on s1.id=friends.friend_id
join packages p on s1.id=p.id
where friends.friend_id=s1.id)

select ab.name
from ab join s1 
on s1.id=ab.fid 
join packages p on ab.fid=p.id
where ab.salary<p.salary

---------------------------------------------------------------------------------------------------------

create table e (id int,name varchar(10),department varchar(5),managerid int)

INSERT INTO E (id, name, department, managerId) VALUES
(101, 'John', 'A', NULL),
(102, 'Dan', 'A', 101),
(103, 'James', 'A', 101),
(104, 'Amy', 'A', 101),
(105, 'Anne', 'A', 101),
(106, 'Ron', 'B', 101),
(111, 'John', 'A', NULL),
(112, 'Dan', 'A', 111),
(113, 'James', 'A', 111),
(114, 'Amy', 'A', 111),
(115, 'Anne', 'A', 111),
(116, 'Ron', 'B', 111);

with cte as(select e.name,d.managerid from e e join e d on e.id=d.managerid group by e.name,e.managerid  having count(e.id)>=5
)
select name from cte

select * from e
select e1.id,e1.name,e1.managerid,d.name from e e1 join e d on e1.id=d.managerid 

select count(managerid) from e
------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS Hall;
GO

CREATE PROCEDURE Hall
AS
BEGIN
    DECLARE @stdid INT=1, @deptid INT, @deptname VARCHAR(5);
    DECLARE @collegeid INT=1, @collegecode VARCHAR(5), @courseid INT=1, @coursecode VARCHAR(5), @coursename VARCHAR(10);
    DECLARE @scholar_code INT, @scholartype VARCHAR(5), @year INT, @scholarid INT=1, @yearid INT=1,@hallticket_no varchar(25),@i int;


	 DECLARE dept_cursor CURSOR FOR
    SELECT dept_id FROM dept;

    OPEN dept_cursor;
    FETCH NEXT FROM dept_cursor INTO @deptid;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @stdid = 1;
        WHILE @stdid <= 10
        BEGIN

    SELECT @stdid = studentid, @deptid = deptid, @collegeid = collegeid, @courseid = courseid, @yearid = year_id
    FROM student ;

    SELECT @year = years FROM years WHERE year_id = @yearid;
    SELECT @deptid = dept_id,@deptname=dept_name FROM dept WHERE dept_id=@deptid;
    SELECT @collegecode = college_code FROM college WHERE collegeid = @collegeid;
    SELECT @coursecode = course_code FROM course WHERE courseid=@courseid;
	select @scholar_code=scholar_code  from scholar where scholarid=@scholarid;

    WITH cte AS (
        SELECT
            studentid,
            ROW_NUMBER() OVER (PARTITION BY deptname, collegeid, year_id, courseid,scholar_id ORDER BY studentid) AS row_num
        FROM student
    )
    UPDATE r
    SET hallticket_no = CONCAT(
        RIGHT(@year, 2),
        @collegecode,
        @scholar_code,
        @coursecode,
        RIGHT('00' + CAST(@deptid AS VARCHAR(2)), 2),
        RIGHT('00' + CAST(c.row_num AS VARCHAR(2)), 2)
    )
    FROM student r
    JOIN cte c ON r.studentid = c.studentid
    WHERE r.studentid = @stdid;

	update student set deptname=@deptname where deptid=@deptid;
	
	select @hallticket_no=hallticket_no from student where studentid=@stdid;

	INSERT INTO student (deptid, collegeid, courseid, scholar_id, year_id, hallticket_no)
            VALUES (@deptid, @collegeid, @courseid, @scholarid, @yearid, @hallticket_no);
            SET @stdid = @stdid + 1;
        END
        FETCH NEXT FROM dept_cursor INTO @deptid;
    END

    CLOSE dept_cursor;
    DEALLOCATE dept_cursor;
END;
    
	exec hall
	select * from student


insert into student (deptid,courseid,scholar_id,year_id,collegeid) values(1,1,1,1,1)

exec hall
drop proc hall
select * from student
----------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS InsertRandomMarksForStudents;
GO

-- Create the procedure to insert random marks for each student
CREATE PROCEDURE InsertRandomMarksForStudents
AS
BEGIN
    DECLARE @studentid INT;
    DECLARE @subject_id INT;
    DECLARE @random_marks INT;
    DECLARE @semester INT;

    DECLARE student_cursor CURSOR FOR
    SELECT studentid FROM student;

    DECLARE subject_cursor CURSOR FOR
    SELECT subject_id FROM subject;

    OPEN student_cursor;
    FETCH NEXT FROM student_cursor INTO @studentid;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        OPEN subject_cursor;
        FETCH NEXT FROM subject_cursor INTO @subject_id;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @random_marks = CAST(RAND() * 100 AS INT);  -- Marks between 0 and 100
            SET @semester = ABS(CHECKSUM(NEWID())) % 4 + 1; -- Assuming there are 4 semesters

            INSERT INTO marks (studentid, subject_id, marks, semester)
            VALUES (@studentid, @subject_id, @random_marks, @semester);
			exec spGetPassFail

            FETCH NEXT FROM subject_cursor INTO @subject_id;
        END;

        CLOSE subject_cursor;
        FETCH NEXT FROM student_cursor INTO @studentid;
    END;

    CLOSE student_cursor;
    DEALLOCATE student_cursor;
    DEALLOCATE subject_cursor;
END;
GO

-- Execute the procedure to insert random marks for each student
EXEC InsertRandomMarksForStudents;

--------------------------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS Hall;
GO

CREATE PROCEDURE Hall
AS
BEGIN
    DECLARE @stdid INT, @deptid INT, @deptname VARCHAR(5);
    DECLARE @collegeid INT, @collegecode VARCHAR(5), @courseid INT, @coursecode VARCHAR(5), @coursename VARCHAR(10);
    DECLARE @scholar_code INT, @scholartype VARCHAR(5), @year INT, @scholarid INT, @yearid INT;

    SELECT @stdid = studentid, @deptname = deptname, @collegeid = collegeid, @coursename = coursename, @yearid = year_id
    FROM student ;

    SELECT @year = years FROM years WHERE year_id = @yearid;
    SELECT @deptid = deptid FROM department WHERE deptname = @deptname;
    SELECT @collegecode = collegecode FROM college WHERE collegeid = @collegeid;
    SELECT @coursecode = course_code, @courseid = courseid FROM course WHERE course_name = @coursename;

	select @scholar_code=s1.scholar_code,@scholarid= s1.scholarid  from student s join scholar s1 on s.scholar_type=s1.scholar_type;

    WITH cte AS (
        SELECT
            studentid,
            ROW_NUMBER() OVER (PARTITION BY deptname, collegeid, year_id, coursename, scholar_type ORDER BY studentid) AS row_num
        FROM student
    )
    UPDATE r
    SET hallticket_no = CONCAT(
        RIGHT(@year, 2),
        @collegecode,
        @scholar_code,
        @coursecode,
        RIGHT('00' + CAST(@deptid AS VARCHAR(2)), 2),
        RIGHT('00' + CAST(c.row_num AS VARCHAR(2)), 2)
    )
    FROM student r
    JOIN cte c ON r.studentid = c.studentid
    WHERE r.studentid = @stdid;

    INSERT INTO fact (deptid, collegeid, courseid, scholarid,year_id,years,studentid)
    VALUES (@deptid, @collegeid, @courseid, @scholarid,@yearid, @year,@stdid);
END;
GO

-- Insert a student and execute the stored procedure
INSERT INTO student (student_name, deptname, collegeid, coursename, scholar_type, year_id, hallticket_no)
VALUES ('Raj', 'ece', 1, 'b.tech', 'lateral', 4, NULL);

EXEC Hall;

INSERT INTO student (student_name, deptname, collegeid, coursename, scholar_type, year_id, hallticket_no)
VALUES ('santhosh', '', 3,'b.tech', 'lateral', 40, NULL);

select * from student
select * from fact

create table marks(
	mark_id int not null primary key identity(1,1),
	studentid int,
	subject_id int,
	marks int,
	semester int,
	passFailStatus nvarchar(10),
    foreign key (studentid) references student(studentid),
	foreign key (subject_id) references subject(subject_id)
);
drop table marks

--------------------------------------------------------------------------
set @mark1 = CAST(RAND() * 100 AS INT);  -- Marks between 0 and 100
			set @mark2=cast(rand()*100 as int);

			if @mark1>=40
				begin
					if @mark2>=40
					begin
						set @pass='PASS';
						set @semid=@semid+1;
					end
					else
					begin
						set @pass='FAIL';
					end
				end
			else
				begin
					set @pass='FAIL';
				end

				if @i <= 99
            begin
                SET @hallticket_no = CONCAT(
                    RIGHT(@year, 2),
                    @collegecode,
                    @scholar_code,
                    @coursecode,
                    RIGHT('00' + CAST(@deptid AS VARCHAR(2)), 2),
                    RIGHT('00' + CAST(@i AS VARCHAR(2)), 2)
                );
            end
            else
            begin
                SET @hallticket_no = CONCAT(
                    RIGHT(@year, 2),
                    @collegecode,
                    @scholar_code,
                    @coursecode,
                    RIGHT('00' + CAST(@deptid AS VARCHAR(2)), 2),
                    CHAR(65 + ((@i - 100) / 10)) + CAST((@i - 100) % 10 AS VARCHAR(1))
                );
            end

				set @subjectid =(select  top 1 subject_id from subject where deptid=@deptid);
				set @subjectid2=(@subjectid)+1;

				---------------------------------------------------------------------------------------

				19-09-2024


create table table1
(x int,
y int)

insert into table1 values (20,20),(20,20),(20,21),(23,22),(22,23),(21,20)


with cte as (
select x,y , row_number() over (order by x) as rn from table1
   )
   select distinct a.x, a.y from cte a join cte b
   on a.x = b.y and a.y = b.x 
   where a.rn != b.rn and a.x <= a.y
   order by a.x
   ---------------------------------------------------------------------------------------
   SELECT DISTINCT CONVERT(DECIMAL(15,4),PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY LAT_N) OVER ())
FROM STATION

--------------------------------------------------------------------------------------------------
declare @start int
declare @end int
declare @current int
declare @output nvarchar(max)
set @start=2
set @end=1000
set @current=@start

while @current<=@end
begin

with prime as (
select  @start as primenbr
union all
select a.primenbr+1 as value
from prime a
where a.primenbr <@end
),
b as(
select primenbr from prime a 
where not exists 
(select 1 from prime b 
where a.primenbr % b.primenbr=0 
and a.primenbr != b.primenbr)
)

SELECT @output = COALESCE(@output + '&', '') + CAST(primenbr AS NVARCHAR)
    FROM b
    WHERE primenbr = @current
    OPTION (MAXRECURSION 0);
set @current=@current+1
end
select @output as output
-----------------------------------------------------------------------------------

with prime as (
select  1 as primenbr
union all
select a.primenbr+1 as value
from prime a
where a.primenbr <10
)select primenbr from prime;

-------------------------------------------------------------------------------------------------

print'---------------------------------------------'
print('           Patterns-with-sql               ')
print'---------------------------------------------'
declare @i1 int=1;
while @i1<=5
begin
print(rtrim(replicate('* ',@i1)))
set @i1=@i1+1;
end
print'---------------------------------------------'
declare @i2 int=5;
while @i2>=1
begin
print(rtrim(replicate('* ',@i2)))
set @i2=@i2-1;
end
print'---------------------------------------------'
declare @i3 int=5,@j1 int=5;
while @i3>0
begin
DECLARE @formattedString NVARCHAR(MAX);
    SET @formattedString = REPLICATE(' ', @j1 - @i3) + REPLICATE(' *', @i3);
    
    PRINT RIGHT(@formattedString, LEN(@formattedString) - 1);
set @i3=@i3-1;
end
print'---------------------------------------------'
declare @i4 int=5,@j2 int=5;
while @i4>0
begin
DECLARE @formattedString1 NVARCHAR(MAX);
    SET @formattedString1 = REPLICATE('  ', @j2 - @i4) + REPLICATE(' *', @i4);
    
    PRINT RIGHT(@formattedString1, LEN(@formattedString1) - 1);
set @i4=@i4-1;
end
print'---------------------------------------------'
declare @i5 int=1,@j3 int=5;
while @i5<=5
begin
DECLARE @formattedString2 NVARCHAR(MAX);
    SET @formattedString2 = REPLICATE(' ', @j3 - @i5) + REPLICATE(' *', @i5);
    
    PRINT RIGHT(@formattedString2, LEN(@formattedString2) - 1);
set @i5=@i5+1;
end
print'---------------------------------------------'
declare @i6 int=1,@j4 int=5;
while @i6<=5
begin
DECLARE @formattedString3 NVARCHAR(MAX);
    SET @formattedString3 = REPLICATE('  ', @j4) + REPLICATE(' *', @i6);
	    PRINT RIGHT(@formattedString3, LEN(@formattedString3) - 3);

	set @i6=@i6+1;
	set @j4=@j4-1;
	end

print'---------------------------------------------'

----------------------------------------------------------------------------------------
CREATE TABLE Prime_Numbers (number INT);

DECLARE @nr INT;
DECLARE @divider INT;
DECLARE @prime BIT;

SELECT @nr = 1;


WHILE @nr <= 1000
    BEGIN
    SELECT @divider = @nr - 1;
    SELECT @prime = 1;
    -- Prime Number test
    WHILE @divider > 1
        BEGIN
        IF @nr % @divider = 0
            SELECT @prime = 0;
        SELECT @divider = @divider - 1         
        END
    IF @prime = 1 AND @nr <> 1
        INSERT INTO Prime_Numbers (number) VALUES (@nr);
    
    SELECT @nr = @nr + 1
    END

SELECT STRING_AGG(number,'&') FROM Prime_Numbers;

-----------------------------------------------------------------

create table queries (query_name varchar(5),result nvarchar(50),position int,rating int)

insert into queries values('dog','golden retriever',1,5),('dog','german shepherd',2,5),('dog','mule',200,1),('cat','shirazi',5,2),('cat','siamese',3,3),('cat','sphynx',7,4)

select * from queries

with cte as(
select query_name,round(sum(convert(decimal(10,2),rating)/position)/count(*),2)as quality 
from queries group by query_name
),
b as(
SELECT query_name,ROUND((CAST(SUM(CASE WHEN rating < 3 THEN 1 ELSE 0 END) AS DECIMAL) / COUNT(*)) * 100, 2) AS per
FROM queries group by query_name)
select c.query_name,c.quality,b.per as poor_query_percentage from cte c join b on c.query_name=b.query_name;

-------------------------------------------------------------------------------------------------------------------
create table teacher(teacherid int,subjectid int,departmentid int)

insert into teacher values(1,2,3),(1,2,4),(1,3,3),(2,1,1),(2,2,1),(2,3,1),(2,4,1)


select teacherid,count(distinct(subjectid))as cnt from teacher  group by teacherid 

------------------------------------------------------------------------------------------------------------------
create table person (id int,email nvarchar(25))

insert into person values (1,'abc@gmail.com'),(2,'def@gmail.com'),(3,'abc@gmail.com')

delete id from(select  id,email,count(*) over (partition by email order by id )as r from person)as a where r>1

------------------------------------------------------------------------------------------------------------------

CREATE TABLE Signups (
    user_id INT,
    time_stamp DATETIME
);

INSERT INTO Signups (user_id, time_stamp) VALUES
(3, '2020-03-21 10:16:13'),
(7, '2020-01-04 13:57:59'),
(2, '2020-07-29 23:09:44'),
(6, '2020-12-09 10:39:37');

CREATE TABLE Confirmations (
    user_id INT,
    time_stamp DATETIME,
    action VARCHAR(50)
);

INSERT INTO Confirmations (user_id, time_stamp, action) VALUES
(3, '2021-01-06 03:30:46', 'timeout'),
(3, '2021-07-14 14:00:00', 'timeout'),
(7, '2021-06-12 11:57:29', 'confirmed'),
(7, '2021-06-13 12:58:28', 'confirmed'),
(7, '2021-06-14 13:59:27', 'confirmed'),
(2, '2021-01-22 00:00:00', 'confirmed'),
(2, '2021-02-28 23:59:59', 'timeout');


with cte as(
select s.*,c.time_stamp as tm,c.action from Signups s left join Confirmations c on s.user_id=c.user_id
)
select USER_ID,case when count(action)>=2 then 1 when count(action) is null then 0 else 0 end from cte where case when action='confirmed' then action else 0 end group by user_id 

------------------------------------------------------------------------------------------------------------------------

SELECT p.product_id,COALESCE(ROUND(CAST(SUM(p.price * Us.units) AS FLOAT)/SUM(Us.units),2),0) AS average_price
FROM Prices p
LEFT join UnitsSold Us
ON p.product_id = Us.product_id
    AND Us.purchase_date  BETWEEN p.start_date AND p.end_date  
GROUP BY p.product_id

-----------------------------------------------------------------------------------------------------------

create table logs
(id int identity(1,1),
num int)

insert into logs values (1),(1),(1),(2),(1),(2),(2)

select * from logs

select num from (SELECT  id, num ,row_number() OVER (partition by num,id ORDER BY id) AS prev_num FROM logs)as a where prev_num>=3

with base as(
select 
   id, 
   num, 
   lag(num,1)over(order by id) as lag1, 
   lag(num,2) over(order by id)as lag2
from 
    logs )

select distinct 
    num as ConsecutiveNums  
from 
    base 
where 
    num = lag1 and
    num = lag2
--------------------------------------------------------------------------------

-- Create the Activity table
CREATE TABLE Activity (
    user_id INT,
    session_id INT,
    activity_date DATE,
    activity_type VARCHAR(50)
);

-- Insert the given values into the Activity table
INSERT INTO Activity (user_id, session_id, activity_date, activity_type)
VALUES 
(1, 1, '2019-07-20', 'open_session'),
(1, 1, '2019-07-20', 'scroll_down'),
(1, 1, '2019-07-20', 'end_session'),
(2, 4, '2019-07-20', 'open_session'),
(2, 4, '2019-07-21', 'send_message'),
(2, 4, '2019-07-21', 'end_session'),
(3, 2, '2019-07-21', 'open_session'),
(3, 2, '2019-07-21', 'send_message'),
(3, 2, '2019-07-21', 'end_session'),
(4, 3, '2019-06-25', 'open_session'),
(4, 3, '2019-06-25', 'end_session');





select Activity_date as day,count(distinct user_id) as active_users from activity
where activity_date<='2019-07-27' and DATEDIFF(day,activity_date,'2019-07-27')<30 group by activity_date

-----------------------------------------------------

create table users
(id int,
name varchar(10)
)

insert into users values (1,'aLice'),(2,'bOB')

select concat(upper(substring(name,1,1)),lower(substring(name,2,len(name))))as name from users

-----------------------------------------------------------------------------------------------

-- Create the Activities table
CREATE TABLE Activities (
    sell_date DATE,
    product VARCHAR(50)
);

-- Insert the given values into the Activities table
INSERT INTO Activities (sell_date, product)
VALUES 
('2020-05-30', 'Headphone'),
('2020-06-01', 'Pencil'),
('2020-06-02', 'Mask'),
('2020-05-30', 'Basketball'),
('2020-06-01', 'Bible'),
('2020-06-02', 'Mask'),
('2020-05-30', 'T-Shirt');


--------------------------------------------------------------------------------
SELECT 
    *
FROM
    USERS
WHERE
    RIGHT(MAIL,13) = '@leetcode.com' AND 
    LEFT(MAIL,1) LIKE '[a-zA-Z]%' AND
    LEFT(MAIL, LEN(MAIL) - 13) NOT LIKE '%[^a-zA-Z0-9_.-]%';


	------------------------------------------------------------------------------------------
	-- Create Movies table
CREATE TABLE Movies (
    movie_id INT PRIMARY KEY,
    title NVARCHAR(100)
);

-- Create Users table
CREATE TABLE Users (
    user_id INT PRIMARY KEY,
    name NVARCHAR(100)
);

-- Create MovieRating table
CREATE TABLE MovieRating (
    movie_id INT,
    user_id INT,
    rating INT,
    created_at DATE,
    PRIMARY KEY (movie_id, user_id),
    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);

-- Insert data into Movies table
INSERT INTO Movies (movie_id, title) VALUES
(1, 'Avengers'),
(2, 'Frozen 2'),
(3, 'Joker');

-- Insert data into Users table
INSERT INTO Users (user_id, name) VALUES
(1, 'Daniel'),
(2, 'Monica'),
(3, 'Maria'),
(4, 'James');

-- Insert data into MovieRating table
INSERT INTO MovieRating (movie_id, user_id, rating, created_at) VALUES
(1, 1, 3, '2020-01-12'),
(1, 2, 4, '2020-02-11'),
(1, 3, 2, '2020-02-12'),
(1, 4, 1, '2020-01-01'),
(2, 1, 5, '2020-02-17'),
(2, 2, 2, '2020-02-01'),
(2, 3, 2, '2020-03-01'),
(3, 1, 3, '2020-02-22'),
(3, 2, 4, '2020-02-25');

select * from users

select m.title,m1.*,u.name from movies m join MovieRating m1 on m.movie_id=m1.movie_id 
join users u on u.user_id=m1.user_id 
where (m1.created_at <'2020-03-1' and m1.created_at >='2020-02-1') and m1.rating>3 


select top 1 name as results
    from (
        select u.user_id, u.name, m.title, mr.rating
        from MovieRating mr
        join Users u on mr.user_id = u.user_id
        join Movies m on mr.movie_id = m.movie_id
        ) new1
    group by new1.user_id, new1.name
    order by count(new1.user_id) desc, new1.name
	union all
	select top 1 title as results
    from (
        select m.title, avg(mr.rating*1.0) as rating
        from MovieRating mr 
        join Movies m on mr.movie_id = m.movie_id 
        where mr.created_at between '2020-02-01' and '2020-02-29'
        group by mr.movie_id, m.title
        ) new2
    order by new2.rating desc, new2.title

	--------------------------------------------------------------------------------

	-- Create Customer table
CREATE TABLE Customer (
    customer_id INT,
    name NVARCHAR(100),
    visited_on DATE,
    amount INT,
    PRIMARY KEY (customer_id, visited_on)
);

-- Insert data into Customer table
INSERT INTO Customer (customer_id, name, visited_on, amount) VALUES
(1, 'Jhon', '2019-01-01', 100),
(2, 'Daniel', '2019-01-02', 110),
(3, 'Jade', '2019-01-03', 120),
(4, 'Khaled', '2019-01-04', 130),
(5, 'Winston', '2019-01-05', 110),
(6, 'Elvis', '2019-01-06', 140),
(7, 'Anna', '2019-01-07', 150),
(8, 'Maria', '2019-01-08', 80),
(9, 'Jaze', '2019-01-09', 110),
(1, 'Jhon', '2019-01-10', 130),
(3, 'Jade', '2019-01-10', 150);


SELECT * FROM CUSTOMER


select * from customer order by visited_on 


with cte as
(select visited_on,sum(amount)as amount from Customer group by visited_on)
select visited_on,sum(amount) over(order by visited_on rows between 6 preceding and current row )as amount,
round(avg(amount)over (order by visited_on rows between 6 preceding and current row),2) as average_amount
 from cte 
 order by visited_on
 OFFSET 6 rows

 ---------------------------------------------------------------------------------------------------

SELECT 
    FORMAT(trans_date, 'yyyy-MM') AS month,
    country,
    COUNT(id) AS trans_count,
    SUM(CASE WHEN state = 'approved' THEN 1 ELSE 0 END) AS approved_count,
    SUM(amount) AS trans_total_amount,
    SUM(CASE WHEN state = 'approved' THEN amount ELSE 0 END) AS approved_total_amount
FROM 
    transactions
GROUP BY 
    FORMAT(trans_date, 'yyyy-MM'), country;


-------------------------------------------------------------------------------------------------------------

-- Create SalesPerson table
CREATE TABLE SalesPerson (
    sales_id INT PRIMARY KEY,
    name NVARCHAR(100),
    salary INT,
    commission_rate INT,
    hire_date DATE
);

-- Create Company table
CREATE TABLE Company (
    com_id INT PRIMARY KEY,
    name NVARCHAR(100),
    city NVARCHAR(100)
);

drop table Orders
-- Create Orders table
CREATE TABLE Orders1 (
    order_id INT PRIMARY KEY,
    order_date DATE,
    com_id INT,
    sales_id INT,
    amount INT,
    FOREIGN KEY (com_id) REFERENCES Company(com_id),
    FOREIGN KEY (sales_id) REFERENCES SalesPerson(sales_id)
);

-- Insert data into SalesPerson table
INSERT INTO SalesPerson (sales_id, name, salary, commission_rate, hire_date) VALUES
(1, 'John', 100000, 6, '2006-04-01'),
(2, 'Amy', 12000, 5, '2010-05-01'),
(3, 'Mark', 65000, 12, '2008-12-25'),
(4, 'Pam', 25000, 25, '2005-01-01'),
(5, 'Alex', 5000, 10, '2007-02-03');

-- Insert data into Company table
INSERT INTO Company (com_id, name, city) VALUES
(1, 'RED', 'Boston'),
(2, 'ORANGE', 'New York'),
(3, 'YELLOW', 'Boston'),
(4, 'GREEN', 'Austin');

-- Insert data into Orders table
INSERT INTO Orders1 (order_id, order_date, com_id, sales_id, amount) VALUES
(1, '2014-01-01', 3, 4, 10000),
(2, '2014-02-01', 4, 5, 5000),
(3, '2014-03-01', 1, 1, 50000),
(4, '2014-04-01', 1, 4, 25000);

with cte as(
select s.*,o.order_id,o.amount,o.com_id,o.order_date from SalesPerson s  join Orders1 o on s.sales_id=o.sales_id
)


SELECT name
FROM SalesPerson
EXCEPT
SELECT s.name
FROM Orders1 o
JOIN Company c ON o.com_id = c.com_id
JOIN SalesPerson s ON o.sales_id = s.sales_id
WHERE c.name = 'RED'

select name
from SalesPerson
where sales_id NOT IN (select sales_id
                from Orders1
                where com_id IN (select com_id
                            from Company
                            where name='RED'))


---------------------------------------------------------------------
-- Create the table
CREATE TABLE ActorDirector (
    actor_id INT,
    director_id INT,
    timestamp INT,
    PRIMARY KEY (actor_id, director_id, timestamp)
);

-- Insert data into the table
INSERT INTO ActorDirector (actor_id, director_id, timestamp) VALUES
(1, 1, 0),
(1, 1, 1),
(1, 1, 2),
(1, 2, 3),
(1, 2, 4),
(2, 1, 5),
(2, 1, 6);

select actor_id, director_id
from ActorDirector
group by actor_id, director_id
having count(timestamp) >= 3

select contest_id, 
round(count(user_id) * 100.0 / (select count(user_id) from Users), 2) as percentage
from Register
group by contest_id
order by percentage desc, contest_id;
----------------------------------------------------------------------------
Employee table:
+----+-------+--------+--------------+
| id | name  | salary | departmentId |
+----+-------+--------+--------------+
| 1  | Joe   | 70000  | 1            |
| 2  | Jim   | 90000  | 1            |
| 3  | Henry | 80000  | 2            |
| 4  | Sam   | 60000  | 2            |
| 5  | Max   | 90000  | 1            |
+----+-------+--------+--------------+
Department table:
+----+-------+
| id | name  |
+----+-------+
| 1  | IT    |
| 2  | Sales |
+----+-------+


-- Create Employee table
CREATE TABLE Employee11 (
    id INT PRIMARY KEY,
    name NVARCHAR(100),
    salary INT,
    departmentId INT,
    FOREIGN KEY (departmentId) REFERENCES Department(id)
);

-- Create Department table
CREATE TABLE Department (
    id INT PRIMARY KEY,
    name NVARCHAR(100)
);

-- Insert data into Department table
INSERT INTO Department (id, name) VALUES
(1, 'IT'),
(2, 'Sales');

-- Insert data into Employee table
INSERT INTO Employee11 (id, name, salary, departmentId) VALUES
(1, 'Joe', 70000, 1),
(2, 'Jim', 90000, 1),
(3, 'Henry', 80000, 2),
(4, 'Sam', 60000, 2),
(5, 'Max', 90000, 1);


with cte as(
select e.*,d.name as dpt from employee11 e left join department d on e.departmentid=d.id
)
select * from(select  dpt,name,salary,rank() over (partition by dpt order by salary desc) r from cte group by dpt,name,salary )as a where r=1

---------------------------------------------------------------------------------------------------------------------------------------

Employee table:
+----+-------+--------+--------------+
| id | name  | salary | departmentId |
+----+-------+--------+--------------+
| 1  | Joe   | 85000  | 1            |
| 2  | Henry | 80000  | 2            |
| 3  | Sam   | 60000  | 2            |
| 4  | Max   | 90000  | 1            |
| 5  | Janet | 69000  | 1            |
| 6  | Randy | 85000  | 1            |
| 7  | Will  | 70000  | 1            |
+----+-------+--------+--------------+

INSERT INTO Employee11 (id, name, salary, departmentId) VALUES
(1, 'Joe', 85000, 1),
(2, 'henry', 80000, 2),
(3, 'Sam', 60000, 2),
(4, 'max', 90000, 1),
(5, 'janet', 69000, 1),
(6,'Randy',85000,1),
(7,'will',70000,1);
Department table:
+----+-------+
| id | name  |
+----+-------+
| 1  | IT    |
| 2  | Sales |
+----+-------+

select * from employee11

with cte as(
select e.*,d.name as dpt from employee11 e left join department d on e.departmentid=d.id
)
select * from(select  dpt,name,salary,dense_rank() over (partition by dpt order by salary desc) r from cte group by dpt,name,salary )as a where r<4
----------------------------------------------------------------------------------------------------------------------------------------------------

SELECT id, CASE
        WHEN id % 2 = 0 THEN LAG(student) OVER(ORDER BY id)
        ELSE COALESCE(LEAD(student) OVER(ORDER BY id), student)
    END AS student
FROM Seat
-----------------------------------------------------------------------------------------------------------------------
with cte AS(
    select pid,
        tiv_2015,
        tiv_2016 ,
        count(pid)OVER(partition by tiv_2015 )AS tv_cnt,
        count(pid)OVER(partition by lat,lon)AS l_cnt 
from insurance
)
select round(sum(tiv_2016),2)AS tiv_2016  from cte where tv_cnt >1 and l_cnt <2
------------------------------------------------------------------------------------------------------------------

-- Create Accounts table
CREATE TABLE Accounts (
    account_id INT PRIMARY KEY,
    income INT
);

-- Insert data into Accounts table
INSERT INTO Accounts (account_id, income) VALUES
(3, 108939),
(2, 12747),
(8, 87709),
(6, 91796);

"Low Salary": All the salaries strictly less than $20000.
"Average Salary": All the salaries in the inclusive range [$20000, $50000].
"High Salary": All the salaries strictly greater than $50000.

SELECT 'High Salary' as category, COUNT( CASE WHEN income > 50000 THEN income ELSE null END) as accounts_count FROM Accounts
UNION
SELECT 'Low Salary' as category, COUNT( CASE WHEN income < 20000 THEN income ELSE null END) as accounts_count FROM Accounts
UNION
SELECT 'Average Salary' as category, COUNT( CASE WHEN income >= 20000 and income <= 50000 THEN income ELSE null END) as accounts_count FROM Accounts ORDER BY accounts_count DESC

---------------------------------------------------------------------------------------

-- Create Activity table
CREATE TABLE Activity (
    player_id INT,
    device_id INT,
    event_date DATE,
    games_played INT,
    PRIMARY KEY (player_id, device_id, event_date)
);

-- Insert data into Activity table
INSERT INTO Activity (player_id, device_id, event_date, games_played) VALUES
(1, 2, '2016-03-01', 5),
(1, 2, '2016-03-02', 6),
(2, 3, '2017-06-25', 1),
(3, 1, '2016-03-02', 0),
(3, 4, '2018-07-03', 5);

select * from activity


  WITH FirstLogin AS (
    SELECT 
        player_id, 
        MIN(event_date) AS first_login 
    FROM 
        Activity 
    GROUP BY 
        player_id
)
SELECT 
    ROUND(CAST(COUNT(DISTINCT A.player_id) AS FLOAT) / 
          (SELECT COUNT(DISTINCT player_id) FROM Activity), 2) AS fraction
FROM 
    Activity A
JOIN 
    FirstLogin F ON A.player_id = F.player_id 
WHERE 
    A.event_date = DATEADD(DAY, 1, F.first_login);

	-----------------------------------------------------------------------------------------------------

	create table hacker 
	(
	hacker_id int,
	name varchar(10)
	)

	INSERT INTO Hacker (hacker_id, name) VALUES
(5077, 'Rose'),
(21283, 'Angela'),
(62743, 'Frank'),
(88255, 'Patrick'),
(96196, 'Lisa');

create table challenges
(
challenges_id int ,
hacker_id int
)

INSERT INTO Challenges (Challenges_Id, Hacker_Id) VALUES
(61654, 5077),
(58302, 21283),
(40587, 88255),
(29477, 5077),
(1220, 21283),
(69514, 21283),
(46561, 62743),
(58077, 62743),
(18483, 88255),
(76766,21283),
(52382, 5077),
(74467, 21283),
(33625, 96196),
(26053, 88255),
(42665,62743),
(12859,62743),
(70094, 21283),
(34599, 88255),
(54680, 88255),
(61881, 5077);





WITH ChallengeCounts AS 
( SELECT h.hacker_id, h.name, COUNT(c.challenges_id) AS total_challenges 
FROM Hacker h LEFT JOIN Challenges c ON h.hacker_id = c.hacker_id 
GROUP BY h.hacker_id, h.name 
), 
MaxChallenges AS ( 
SELECT MAX(total_challenges) AS max_challenges FROM ChallengeCounts
), 
DuplicateCounts AS ( 
SELECT total_challenges FROM ChallengeCounts GROUP BY total_challenges HAVING COUNT(*) > 1 
)

SELECT cc.hacker_id, cc.name, cc.total_challenges FROM ChallengeCounts cc 
WHERE cc.total_challenges = 
(SELECT max_challenges FROM MaxChallenges) OR cc.total_challenges 
NOT IN (SELECT total_challenges FROM DuplicateCounts)
ORDER BY cc.total_challenges DESC, cc.hacker_id;

------------------------------------------------------------------------------------------------

create table contact(empid varchar(10), contact_details varchar(250))

INSERT INTO contact VALUES 
('E001', 'John works at ABC corp. Contact: 9876543210'),
('E002', 'Anna email is anna.smith@gmail.com, Her phone is 9123456789'),
('E003', 'No contact information available.'),
('E004', 'Reach me at 9234567890 or via mail alice.johnson@xyz.co.uk');

 select * from contact

 SELECT 
    empid,
    contact_details,
  
    CASE 
        WHEN contact_details LIKE '%@%' 
        THEN SUBSTRING(contact_details, PATINDEX('[' ']@%', contact_details),10)
        ELSE 'null'
    END AS email_address
FROM contact;


select empid,(case when contact_details like '%[7-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]%' then substring(contact_details,patindex('%[7-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]%',contact_details),10) else null end) as mobile,
(case when contact_details  LIKE ' %@% ' then substring(contact_details,patindex(' %@% ',contact_details),20) else null end) as email from contact


SELECT 
    empid,
    contact_details,
    CASE 
        WHEN PATINDEX('%[A-Za-z0-9._%+-]@[A-Za-z0-9.-]%.[A-Za-z]{2,}%', contact_details) > 0 
        THEN SUBSTRING(
            contact_details, 
            PATINDEX('%[A-Za-z0-9._%+-]@[A-Za-z0-9.-]%.[A-Za-z]{2,}%', contact_details), 
            LEN(contact_details) - PATINDEX('%[A-Za-z0-9._%+-]@[A-Za-z0-9.-]%.[A-Za-z]{2,}%', contact_details) + 1
        )
        ELSE 'None'
    END AS email_address
FROM contact;




	WITH cte AS (
    SELECT 
        empid,
        MAX(CASE WHEN value LIKE '%[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]%' THEN value ELSE NULL END) AS mobile,
        MAX(CASE WHEN value LIKE '%@%' THEN value ELSE NULL END) AS email
    FROM 
        contact
    CROSS APPLY 
        STRING_SPLIT(REPLACE(contact_details, ',', ' '), ' ')
    GROUP BY 
        empid
)
SELECT 
    empid, 
    mobile, 
    email
FROM 
    cte;

	-------------------------------------------------------------------------------------------------------------

CREATE TABLE EventLog (
    empid INT,
    event_type NVARCHAR(50),
    event_time DATETIME
);

INSERT INTO EventLog (empid, event_type, event_time) VALUES
(1, 'event_start', '2023-09-01 08:00:00'),
(1, 'event_process', '2023-09-01 08:05:00'),
(1, 'event_complete', '2023-09-01 08:15:00'),
(2, 'event_start', '2023-09-01 10:00:00'),
(2, 'event_process', '2023-09-01 10:05:00'),
(2, 'event_complete', '2023-09-01 10:30:00');

select * from eventlog




with cte as(
select empid,max(case when event_type='event_start' then event_time end)as start,
max(case when event_type='event_complete' then event_time end)as end1
from eventlog
group by empid)
select empid,datediff(minute,start,end1)as duration_minuates from cte


---------------------------------------------------------------------------------------------------

CREATE TABLE Transactions (
    empid INT,
    amounts NVARCHAR(MAX)
);


INSERT INTO Transactions (empid, amounts) VALUES
(1, '[{"amount": 45}, {"amount": 60}]'),
(2, '[{"amount": 30}, {"amount": 20}]'),
(3, '[{"amount": 120}, {"amount": 80}]');

select * from transactions

SELECT 
    empid,
    SUM(CAST(JSON_VALUE(value, '$.amount') AS INT)) AS total_amount
FROM 
    Transactions
CROSS APPLY 
    OPENJSON(amounts)
GROUP BY 
    empid;

----------------------------------------------------------------------------------------
CREATE TABLE EmployeeAmounts (
    empid INT,
    amount INT
);
INSERT INTO EmployeeAmounts (empid, amount) VALUES
(1, 10),
(1, 20),
(1, 30),
(1, 40),
(2, 20),
(2, 40),
(2, 60),
(2, 80);

select empid,amount,sum(amount) over(partition by empid order by amount)as running_total from employeeamounts

--------------------------------------------------------------------------------------------------------------------



