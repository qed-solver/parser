-- Declare table schemas
create table dept(   
  deptno     integer,   
  dname      varchar,   
  loc        varchar,   
  constraint pk_dept primary key (deptno)   
);

create table emp(   
  empno    integer,   
  ename    varchar,   
  job      varchar,   
  mgr      integer,   
  hiredate date,   
  sal      integer,   
  comm     integer,   
  deptno   integer,   
  constraint pk_emp primary key (empno),   
  constraint no_sal check (sal > 0)   
);

-- Declare function signatures
declare scalar function fund(varchar) returns float;
declare aggregate function my_avg(integer) returns float;

-- Define queries (There should only be two!)
select fund(dname) from dept as d join (
  select deptno, my_avg(sal) as avg_sal 
    from emp
    group by deptno
) as s on d.deptno - s.deptno = 0;
select fund(dname) from dept where deptno in (select deptno from emp);
