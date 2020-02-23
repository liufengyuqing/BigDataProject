su - hadoop
cd /home/hadoop/lib
cp -av /tmp/vincent-1.0-SNAPSHOT.jar .
hive
add jar /home/hadoop/lib/vincent-1.0-SNAPSHOT.jar;
-- 将上传的jar包导入到classpath变量里
list jars;
-- 查看导入的jar包
create temporary function say_hello as 'com.vincent.HelloUDF';
-- 创建一个临时函数，关联该jar包
-- as 后面的路径就是上一步获得的全路径
show functions like 'say*';
-- 查看创建的函数
select 'lisi',say_hello('lisi');
-- 测试

以上创建了一个临时的函数，当前会话生效，
hive
add jar /home/hadoop/lib/vincent-1.0-SNAPSHOT.jar;
create function vincent.say_hello1 as 'com.vincent.HelloUDF';
-- 创建一个vincent库的say_hello1函数，永久性的
show functions like 'say*';
select 'lisi',say_hello1('lisi');
-- 当前default是没有该函数的
use vincent;
show functions like 'say*';
select 'lisi',say_hello1('lisi');
-- 此处有一个BUG，show functions无法看到该函数
-- 但是可以直接使用
-- 如果重新在vincent库中创建同名函数，则会报错函数已存在

使用 add jar 命令是当前会话生效的，重新打开一个hive会话，使用vincent.say_hello1报错， 
如果要添加一个永久的函数对应永久的路径，则可以如下操作：

create function vincent.say_hello3 
  as 'com.vincent.HelloUDF' 
  using jar 'hdfs:///tmp/vincent-1.0-SNAPSHOT.jar';
use vincent;
select 'lisi',say_hello3('lisi');
-- 将jar包放到hdfs上，不可以放到OS上，然后创建函数关联路径即可
-- 依然存在 show functions不可见的BUG




UDF的定义：
UDF（User-Defined Functions）即是用户定义的hive函数。
hive自带的函数并不能完全满足业务需求，这时就需要我们自定义函数了

UDF的分类：
UDF：one to one，进来一个出去一个，row mapping。是row级别操作，如：upper、substr函数
UDAF：many to one，进来多个出去一个，row mapping。是row级别操作，如sum/min。
UDTF：one to many ，进来一个出去多个。如alteral view与explode

打jar包package：

上传jar包到hdfs或者hive的服务器

添加jar包到hive：
addhive>add jar +jar包所在的目录/jar包名字 （如果jar包是上传到$HIVE_HOME/lib/目录以下，就不需要执行add命令了） 

在hive中创建UDF函数：