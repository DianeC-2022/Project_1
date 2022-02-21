
import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readInt

object Main {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local").enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    println("created spark session")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE src")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")
    spark.sql("create table if not exists coffee_Branches(beverage String,branch String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Dataset/Bev_BranchA.txt' OVERWRITE INTO TABLE coffee_Branches")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Dataset/Bev_BranchB.txt' INTO TABLE coffee_Branches")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Dataset/Bev_BranchC.txt' INTO TABLE coffee_Branches")
    spark.sql("SELECT * FROM coffee_Branches")

    spark.sql("create table if not exists bev_Conscount(beverage String,count Int) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Dataset/Bev_ConscountA.txt' OVERWRITE INTO TABLE bev_Conscount")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Dataset/Bev_ConscountB.txt' INTO TABLE bev_Conscount")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Dataset/Bev_ConscountC.txt' INTO TABLE bev_Conscount")
    spark.sql("SELECT * FROM bev_Conscount")

    //Problem Scenario 1a
    //What is the total number of consumers for Branch1?
    spark.sql("create table if not exists coffee_BranchA(beverage String,branch String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Dataset/Bev_BranchA.txt' OVERWRITE INTO TABLE coffee_BranchA")

    spark.sql("create table if not exists ConscountA(beverage String,count Int) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Dataset/Bev_ConscountA.txt' OVERWRITE INTO TABLE ConscountA")
    spark.sql("SELECT * FROM ConscountA WHERE 'table' = 'table1'")

    spark.sql("create table if not exists ConsBranch1 (beverage String, count Int) row format delimited fields terminated by ','")
    spark.sql("INSERT into table ConsBranch1 select ConscountA.beverage,count(ConscountA.count) from coffee_BranchA JOIN ConscountA on(coffee_BranchA.beverage = ConscountA.beverage ) group by ConscountA.beverage")
    spark.sql("select sum(count) from ConsBranch1").show()

    //Problem Scenario 1b
    //What is the number of consumers for the Branch2?
    spark.sql("create table if not exists coffee_BranchB(beverage String,branch String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Dataset/Bev_BranchB.txt'OVERWRITE INTO TABLE  coffee_BranchB")

    spark.sql("create table if not exists ConscountB(beverage String,count Int) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Dataset/bev_ConscountB.txt'OVERWRITE INTO TABLE ConscountB")
    spark.sql("SELECT * FROM ConscountB WHERE 'table' = 'table2'")

    spark.sql("create table if not exists ConsBranch2 (beverage String, count Int) row format delimited fields terminated by ','")
    spark.sql("INSERT into table ConsBranch2 select ConscountB.beverage,count(ConscountB.count) from coffee_BranchB JOIN ConscountB on(coffee_BranchB.beverage = ConscountB.beverage ) group by ConscountB.beverage")
    spark.sql("select sum(count) from ConsBranch2").show()

    //Problem Scenario 2a
    //What is the most consumed beverage in Branch2?

    spark.sql(" select beverage,sum(count) count from ConsBranch1 group by beverage order by count desc limit 1;").show()

    //Problem Scenario 2b
    //What is the least consumed beverage in Branch2?
    spark.sql(" select beverage,sum(count) count from ConsBranch2 group by beverage order by count asc limit 1;").show()

    //Problem Scenario 2c
    //What is the average consumed beverage in Branch2?
    spark.sql(" select beverage,avg(count) count from ConsBranch2 group by beverage order by count asc limit 1;").show()

    //Problem Scenario 3a
    //What are the beverages available on Branch10, Branch8, and Branch1?
    spark.sql("Create table if not exists branch10_8_1Beverage (beverage String,branch String")
    spark.sql ("insert into table from coffee_Branches.beverage where branch = 'branch10' and branch = 'branch8' and branch = 'branch1'")
    spark.sql("select * (beverage) from branch10_8_1Beverage order by beverage").show()
    //spark.sql("select distinct beverage from coffee_Branches JOIN bev_Conscount on(coffee_Branches.beverage group by bev_Conscount.beverage where branch ='branch10'or branch ='branch8'or branch='branch1'").show()

    //Problem Scenario 3b
    //What are the common beverages available in Branch 4, Branch 7?
    spark.sql("select beverage from coffee_Branches where branch ='branch4'or branch ='branch7'group by (beverage)").show()

    //Problem Scenario 4
    //Create a partition, view for Scenario 3.
    spark.sql("CREATE TABLE IF NOT EXISTS PartitionT(beverage STRING) PARTITIONED BY (branch STRING)")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("INSERT OVERWRITE TABLE PartitionT PARTITION(branch) SELECT beverage,branch from coffee_Branches")
    spark.sql("SELECT * FROM PartitionT").show()

    //Problem Scenario 5a
    //Alter the table properties to add "note", "comment".
    spark sql("ALTER TABLE PartitionT SET TBLPROPERTIES ('notes'= 'No comment at this time')")
    //Problem Scenario 5b
    //Remove a row from any scenario.
    spark.sql("create table if not exists Q5Delete_Branches_Table(beverage String,branch String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Dataset/Bev_BranchA.txt' OVERWRITE INTO TABLE Q5Delete_Branches_Table")
    spark.sql("CREATE TABLE if not exists Q5Delete_Branches_Table_copy LIKE Q5Delete_Branches_Table")
    spark.sql("Select * from Q5Delete_Branches_Table").show(3)

    //load data into copy table except deleted item
    spark.sql("INSERT INTO Q5Delete_Branches_Table_copy SELECT * FROM Q5Delete_Branches_Table WHERE beverage NOT IN (SELECT beverage FROM Q5Delete_Branches_Table_copy WHERE beverage='Triple_cappuccino')").show()
    //overwrite copy table to original table
    spark.sql("INSERT OVERWRITE TABLE Q5Delete_Branches_Table SELECT * FROM Q5Delete_Branches_Table_copy")
    //drop copy table
    spark.sql("DROP TABLE Q5Delete_Branches_Table")
    //show new table with deleted row
    spark.sql("SELECT * FROM Q5Delete_Branches_Table_copy").show(3)

    //Problem Scenario 6
    //Add a future query.
    //My query is that, by adding specific major cities that are marketing particular beverages and by using the timeframe of 2021, what would be the city that would be beneficial for a new location?
    // In addition, which beverages would be most likely to be consumed in this new branch in 2022?
    spark.sql("create table if not exists coffee_Branches_Cities(beverage String,branch String,city String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input//Copy of Bev_BranchA.txt' OVERWRITE INTO TABLE coffee_Branches_Cities")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Copy of Bev_BranchB.txt' OVERWRITE INTO TABLE coffee_Branches_Cities")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Copy of Bev_BranchC.txt' OVERWRITE INTO TABLE coffee_Branches_Cities")

    spark.sql("create table if not exists ConsBranch (beverage String, count Int) row format delimited fields terminated by ','")
    spark.sql("INSERT into table ConsBranch select bev_Conscount.beverage,count(bev_Conscount.count) from coffee_Branches_Cities JOIN bev_Conscount on(coffee_Branches_Cities.beverage = bev_Conscount.beverage ) group by bev_Conscount.beverage")
    spark.sql(" select beverage,sum(count) count from ConsBranch group by beverage order by count desc").show()







  }


}