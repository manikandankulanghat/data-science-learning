# Essential commands
------------------------------------------------------------------------------------------------
  # Load SparkR
  spark_path <- '/usr/local/spark'
  if (nchar(Sys.getenv("SPARK_HOME")) < 1) 
  {
    Sys.setenv(SPARK_HOME = spark_path)
  }
  library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
  sparkR.session(master = "yarn", sparkConfig = list(spark.driver.memory = "1g"))
  
  # Before executing any hive-sql query from RStudio, you need to add a jar file in RStudio 
  sql("ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar")
  
  library(SparkR)
  library(magrittr)
  library(ggplot2)
  library(sparklyr)
  library(dplyr)
  library(scales)
  -------------------------------------------------------------------------------------------------
  #############################################################
  ###########  Reading data into the environment
  #############################################################  
  
  violations_raw <- SparkR::read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", source = "csv",header = TRUE, inferSchema = TRUE)
  head(violations_raw)
  
  # Examine the size
  nrow(violations_raw)
  # 10803028
  ncol(violations_raw)
  # 10
  
  # Examine the structure
  str(violations_raw)
  
  # Removing space in the column name for ease of coding on subsequent steps & avoid using the ' character always. 
  violations_raw <- withColumnRenamed(violations_raw,"Summons Number", "SummonsNumber")
  violations_raw <- withColumnRenamed(violations_raw,"Plate ID", "PlateID")
  violations_raw <- withColumnRenamed(violations_raw,"Registration State", "RegistrationState")
  violations_raw <- withColumnRenamed(violations_raw,"Issue Date", "IssueDate")
  violations_raw <- withColumnRenamed(violations_raw,"Violation Code", "ViolationCode")
  violations_raw <- withColumnRenamed(violations_raw,"Vehicle Body Type", "VehicleBodyType")
  violations_raw <- withColumnRenamed(violations_raw,"Vehicle Make", "VehicleMake")
  violations_raw <- withColumnRenamed(violations_raw,"Violation Precinct", "ViolationPrecinct")
  violations_raw <- withColumnRenamed(violations_raw,"Issuer Precinct", "IssuerPrecinct")
  violations_raw <- withColumnRenamed(violations_raw,"Violation Time", "ViolationTime")
  
  # Creating a temporary view for accessing using SQL. 
  createOrReplaceTempView(violations_raw, "violations_raw")
  
  # In the file we see 2 columns ViolationTime & IssueDate. We are going to transform that columns to extract the date & time components into individual fields.
  # This would help in easier analysis and plotting in the subsequent steps.  
  violations_time <- SparkR::sql("select year(IssueDate) as IssueYear,
                                 month(IssueDate) as IssueMonth,
                                 date(IssueDate) as IssueDt,
                                 SummonsNumber,
                                 PlateID,
                                 RegistrationState,
                                 IssueDate,
                                 ViolationCode,
                                 VehicleBodyType,
                                 VehicleMake,
                                 ViolationPrecinct,
                                 IssuerPrecinct,
                                 ViolationTime,
                                 SUBSTR(ViolationTime,1,2) as ViolationHour,
                                 SUBSTR(ViolationTime,3,2) as ViolationMinute,
                                 concat(SUBSTR(ViolationTime,5,1),'M') as ViolationPeriod
                                 from violations_raw")
  str(violations_time)
  
  # Creating a temporary view for accessing using SQL. 
  createOrReplaceTempView(violations_time, "violations_time")
  
  #############################################################
  ###########  Examine the data
  #############################################################  
  
  #### 1. How often does each violation code occur? Display the frequency of the top five violation codes.
  # Am extracting distinct SummonsNumber count to avoid any duplicate entries. 
  total_violations <- SparkR::sql("select count(distinct SummonsNumber) 
                                  from violations_time")
  head(total_violations) 
  # 10803028
  #-----------------------------------------------------------#
  
  #### 2. Find out the number of unique states from where the cars that got parking tickets came from. 
  # Group by will ensure we are pulling the distinct state codes. 
  total_states <- SparkR::sql("select count(distinct RegistrationState) as CountStates
                              from violations_time")
  head(total_states)
  # 67 Distinct states exist. 
  
  # Now trying to more information on the count of violations based on registration state. 
  total_states <- SparkR::sql("select RegistrationState,
                              count(*) as Count 
                              from violations_time 
                              group by RegistrationState 
                              order by count(*) desc")
  head(total_states,nrow(total_states))
  # nrow() is used to extract all the rows instead of extracting only the first 6 by default.
  # Based on the ordering desc, we see that NY state plates has the highest number of tickets. 
  
  #### Hint: There is a numeric entry '99' in the column which should be corrected. Replace it with the state having maximum entries. Give the number of unique states again.
  # Based on the above result, we are replacing state code 99 with NY. 
  
  violations_time$RegistrationState <- ifelse(violations_time$RegistrationState %in% c("99"), "NY", violations_time$RegistrationState)
  
  # Rerunning the query confirm the state that has the highest number of violations. 
  total_states <- SparkR::sql("select RegistrationState,count(*) as Count from violations_time group by RegistrationState order by count(*) desc")
  head(total_states,nrow(total_states))
  # Its NY. And since its the NYC parkinig tickets data, its expected that NY state has the highest violations. 
  
  #############################################################
  ###########  Aggregation tasks
  #############################################################
  
  #### 1. How often does each violation code occur? Display the frequency of the top five violation codes.
  top_violation_codes <- SparkR::sql("select ViolationCode,count(*) as Count from violations_time group by ViolationCode order by count(*) desc")
  head(top_violation_codes,5)
  # Head (ds,5) combined with order by desc on count in the SQL will ensure am extracting only the top 5 results. 
  
  # ViolationCode   Count                                                         
  #            21 1528588
  #            36 1400614
  #            38 1062304
  #            14  893498
  #            20  618593 
  #-----------------------------------------------------------#
  
  #### 2.a. How often does each 'vehicle body type' get a parking ticket? Hint: Find the top 5 
  violation_by_vehicle_body_type <- SparkR::sql("select VehicleBodyType,count(*) as Count from violations_time group by VehicleBodyType order by count(*) desc")
  head(violation_by_vehicle_body_type,5)
  # Head (ds,5) combined with order by desc on count in the SQL will ensure am extracting only the top 5 results. 
  
  #VehicleBodyType   Count                                                       
  #           SUBN 3719802
  #           4DSD 3082020
  #            VAN 1411970
  #           DELV  687330
  #            SDN  438191
  
  #### 2.b. How about the 'vehicle make'? Hint: Find the top 5  
  violation_by_vehicle_make <- SparkR::sql("select VehicleMake,count(*) as Count from violations_time group by VehicleMake order by count(*) desc")
  head(violation_by_vehicle_make,5)
  
  #VehicleMake   Count                                                           
  #       FORD 1280958
  #      TOYOT 1211451
  #      HONDA 1079238
  #      NISSA  918590
  #      CHEVR  714655
  
  #-----------------------------------------------------------#
  
  #### 3.1 Find the (5 highest) frequency of tickets for 'Violation Precinct' (this is the precinct of the zone where the violation occurred) 
  top_violation_Precinct <- SparkR::sql("select ViolationPrecinct,count(*) as Count from violations_time group by ViolationPrecinct order by count(*) desc")
  head(top_violation_Precinct,5)
  
  #ViolationPrecinct   Count                                                     
  #                0 2072400
  #               19  535671
  #               14  352450
  #                1  331810
  #               18  306920
  
  # We are seeing precinct = 0 which needs to be ignored based on the following Hint.
  #### Hint: Here you would have noticed that the dataframe has 'Violating Precinct' or 'Issuing Precinct' as '0'. These are the erroneous entries. Hence, provide the record for five correct precincts.  
  top_violation_Precinct <- SparkR::sql("select ViolationPrecinct,count(*) as Count from violations_time where ViolationPrecinct <> '0' group by ViolationPrecinct order by count(*) desc ")
  head(top_violation_Precinct,5)
  # The question said top 5 & the hint said top 6. Am going with top 5 based on what was mentioned in the question. 
  
  #ViolationPrecinct  Count                                                      
  #               19 535671
  #               14 352450
  #                1 331810
  #               18 306920
  #              114 296514
  
  #### 3.2 Find the (5 highest) frequency of tickets for 'Issuer Precinct' (this is the precinct that issued the ticket)
  top_Issuer_Precinct <- SparkR::sql("select IssuerPrecinct,count(*) as Count from violations_time where IssuerPrecinct <> '0' group by IssuerPrecinct order by count(*) desc ")
  head(top_Issuer_Precinct,5)
  
  #IssuerPrecinct  Count                                                         
  #            19 521513
  #            14 344977
  #             1 321170
  #            18 296553
  #           114 289950
  
  #-----------------------------------------------------------#
  
  #### 4.a. Find the violation code frequency across three precincts which have issued the most number of tickets. 
  
  top_3_Issuer_Precinct <- SparkR::sql("select IssuerPrecinct,count(*) as Count from violations_time where IssuerPrecinct <> '0' group by IssuerPrecinct order by count(*) desc LIMIT 3")
  head(top_3_Issuer_Precinct)
  
  #IssuerPrecinct  Count                                                         
  #            19 521513
  #            14 344977
  #             1 321170
  
  # Creating a temporary view for accessing using SQL.  
  createOrReplaceTempView(top_3_Issuer_Precinct, "top_3_Issuer_Precinct")
  
  #### 4.b Do these precinct zones have an exceptionally high frequency of certain violation codes? Are these codes common across precincts? 
  # The Hint mentioned about using where clause. Am using where clause to join the 2 temp views. Extracting the total count by precinct & the percentage of overall tickets it contributes. 
  precinct_violation_analysis <- SparkR::sql("select B.ViolationCode,
                                             A.IssuerPrecinct,
                                             count(B.*) as ticket_count,
                                             round((count(B.*)/a.Count)*100,2) as perc_ticket_count
                                             from top_3_Issuer_Precinct as A,
                                             violations_time as B
                                             where A.IssuerPrecinct = B.IssuerPrecinct
                                             group by A.IssuerPrecinct,
                                             a.Count,
                                             B.ViolationCode 
                                             order by count(B.*) desc,
                                             A.IssuerPrecinct
                                             ")
  head(precinct_violation_analysis,nrow(precinct_violation_analysis))
  #Precinct 19 issues 16.57 % of its 521513 tickets in violation code 46. Based on https://data.cityofnewyork.us/Transportation/DOF-Parking-Violation-Codes/ncbg-6agr violation code 46 stands for "DOUBLE PARKING". 
  
  # filtering onlu the Violation Code 46 & 14 to understand their occurances in other precincts. 
  precinct_top_violation <- SparkR::sql("select * from (select B.ViolationCode,
                                        A.IssuerPrecinct,
                                        count(B.*) as ticket_count,
                                        round((count(B.*)/a.Count)*100,2) as perc_ticket_count
                                        from top_3_Issuer_Precinct as A,
                                        violations_time as B
                                        where A.IssuerPrecinct = B.IssuerPrecinct
                                        group by A.IssuerPrecinct,
                                        a.Count,
                                        B.ViolationCode 
                                        order by count(B.*) desc,
                                        A.IssuerPrecinct)
                                        where ViolationCode in (46,14)
                                        ")
  head(precinct_top_violation,nrow(precinct_top_violation))
  #ViolationCode IssuerPrecinct ticket_count perc_ticket_count                   
  #           46             19        86390             16.57
  #           14             14        73837             21.40
  #           14              1        73522             22.89
  #           14             19        57563             11.04
  #           46              1        22534              7.02
  #           46             14        13435              3.89
  #Violation code 46 is not frequent in other precincts. For precincts 14 & 1, their top violation code for ticket is 14 which stands for "NO STANDING-DAY/TIME LIMITS".
  
  #-----------------------------------------------------------#
  
  #### 5. You’d want to find out the properties of parking violations across different times of the day
  #### 5.1 Find a way to deal with missing values, if any.
  #Finding Missing Values using isnull() & "value is null" options & taking the sum to get the totals. 
  #Reviewed the dropNA() function usage. However, sinve no nulls were found, skipping the dropNA function
  Null_Count <- SparkR::sql("select sum(case when IssueYear is null then 1 else 0 end) as Nulls_IssueYear,
                            sum(case when isnull(IssueMonth) then 1 else 0 end) as Nulls_IssueMonth,
                            sum(case when isnull(IssueDt) then 1 else 0 end) as Nulls_IssueDt,
                            sum(case when SummonsNumber is null then 1 else 0 end) as Nulls_SummonsNumber,
                            sum(case when isnull(PlateID) then 1 else 0 end) as Nulls_PlateID,
                            sum(case when isnull(RegistrationState) then 1 else 0 end) as Nulls_RegistrationState,
                            sum(case when isnull(IssueDate) then 1 else 0 end) as Nulls_IssueDate,
                            sum(case when isnull(ViolationCode) then 1 else 0 end) as Nulls_ViolationCode,
                            sum(case when isnull(VehicleBodyType) then 1 else 0 end) as Nulls_VehicleBodyType,
                            sum(case when isnull(VehicleMake) then 1 else 0 end) as Nulls_VehicleMake,
                            sum(case when isnull(ViolationPrecinct) then 1 else 0 end) as Nulls_ViolationPrecinct,
                            sum(case when isnull(IssuerPrecinct) then 1 else 0 end) as Nulls_IssuerPrecinct,
                            sum(case when isnull(ViolationTime) then 1 else 0 end) as Nulls_ViolationTime,
                            sum(case when isnull(ViolationHour) then 1 else 0 end) as Nulls_ViolationHour,
                            sum(case when isnull(ViolationMinute) then 1 else 0 end) as Nulls_ViolationMinute,
                            sum(case when isnull(ViolationPeriod) then 1 else 0 end) as Nulls_ViolationPeriod
                            from violations_time")
  head(Null_Count,nrow(Null_Count))
  # We don't see any null counts here. 
  
  #Nulls_IssueYear Nulls_IssueMonth Nulls_IssueDt Nulls_SummonsNumber Nulls_PlateID Nulls_RegistrationState
  #1               0                0             0                   0             0                       0
  #Nulls_IssueDate Nulls_ViolationCode Nulls_VehicleBodyType Nulls_VehicleMake Nulls_ViolationPrecinct
  #1               0                   0                     0                 0                       0
  #Nulls_IssuerPrecinct Nulls_ViolationTime Nulls_ViolationHour Nulls_ViolationMinute Nulls_ViolationPeriod
  #1                    0                   0                   0                     0                     0
  
  #### 5.2 The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.
  #Since we have previously split the time into hours minutes & period, we are going to validate the records here. 
  distinct_violation_period <- SparkR::sql("select ViolationPeriod,count(*) as count from violations_time group by ViolationPeriod")
  head(distinct_violation_period,nrow(distinct_violation_period))
  # Here we see that there are records that have the period other than 'AM' or 'PM' Thus removing the same from next steps. 
  
  #ViolationPeriod   count                                                       
  #             PM 5270552
  #             AM 5532391
  #              M      84
  #             0M       1
  
  distinct_violation_hour <- SparkR::sql("select ViolationHour,count(*) as count from violations_time where ViolationPeriod in ('AM','PM') group by ViolationHour")
  head(distinct_violation_hour,nrow(distinct_violation_hour))
  # Here we see that we have records that are having time in hours listed other than 00,01,02,03,04,05,06,07,08,09,10,11 & 12. 
  # However all the incorrect values are having less than 10 records. Thus removing them by introducing having clause in the query. 
  
  distinct_violation_hour <- SparkR::sql("select ViolationHour,count(*) as count from violations_time where ViolationPeriod in ('AM','PM') group by ViolationHour having count(*) >= 10")
  head(distinct_violation_hour,nrow(distinct_violation_hour))
  
  # Removing the data where period is not in AM/PM & the hours is not between 00 - 12 by explicitly filtering them using where clause. 
  violations_time_fixed <- SparkR::sql("select * from violations_time where ViolationPeriod in ('AM','PM') and ViolationHour in ('00','01','02','03','04','05','06','07','08','09','10','11','12')")
  head(violations_time_fixed)
  nrow(violations_time_fixed)
  nrow(violations_time) # Comparing against the count before removing the rejects. 
  
  # Creating a temporary view for accessing using SQL. 
  createOrReplaceTempView(violations_time_fixed, "violations_time_fixed")
  
  #### 5.3 Divide 24 hours into six equal discrete bins of time. The intervals you choose are at your discretion. For each of these groups, find the three most commonly occurring violations.
  #### Hint: Use the CASE-WHEN in SQL view to segregate into bins. For finding the most commonly occurring violations, a similar approach can be used as mention in the hint for question 4.
  
  # Here we are using the case statement to split the following timezones. 
  # 03:00 AM - 06:59 AM - Dawn
  # 07:00 AM - 10:59 AM - Morning
  # 11:00 AM - 02:59 PM - Afternoon
  # 03:00 PM - 06:59 PM - Evening
  # 07:00 PM - 10:59 PM - Night
  # 11:00 PM - 02:59 AM - Midnight
  
  violations_time_bin <- SparkR::sql("select IssueYear,
                                     IssueMonth,
                                     IssueDt,
                                     SummonsNumber,
                                     PlateID,
                                     RegistrationState,
                                     IssueDate,
                                     ViolationCode,
                                     VehicleBodyType,
                                     VehicleMake,
                                     VehicleMake,
                                     ViolationPrecinct,
                                     IssuerPrecinct,
                                     ViolationTime,
                                     ViolationHour,
                                     ViolationMinute,
                                     ViolationPeriod,
                                     case when ((ViolationHour = '03' or ViolationHour = '04' 
                                     or ViolationHour = '05' or ViolationHour = '06') 
                                     and ViolationPeriod = 'AM') THEN 'Dawn' 
                                     when ((ViolationHour = '07' or ViolationHour = '08' 
                                     or ViolationHour = '09' or ViolationHour = '10') 
                                     and ViolationPeriod = 'AM') THEN 'Morning' 
                                     when ((ViolationHour = '11' and ViolationPeriod = 'AM')  
                                     or ((ViolationHour = '12' or ViolationHour = '01' or ViolationHour = '02') 
                                     and ViolationPeriod = 'PM')) THEN 'Afternoon' 
                                     when ((ViolationHour = '03' or ViolationHour = '04' 
                                     or ViolationHour = '05' or ViolationHour = '06') 
                                     and ViolationPeriod = 'PM') THEN 'Evening' 
                                     when ((ViolationHour = '07' or ViolationHour = '08' 
                                     or ViolationHour = '09' or ViolationHour = '10') 
                                     and ViolationPeriod = 'PM') THEN 'Night' 
                                     else 'Midnight' end as TimeOfDay 
                                     from violations_time_fixed")
  
  head(violations_time_bin)
  
  # Creating a temporary view for accessing using SQL. 
  createOrReplaceTempView(violations_time_bin, "violations_time_bin")
  
  # For each of these groups, find the three most commonly occurring violations.
  tickets_by_time_of_day <- SparkR::sql("select * 
                                        from (select TimeOfDay,
                                        ViolationCode,
                                        TicketCount,
                                        dense_rank () over (partition by TimeOfDay order by TicketCount desc) Rank
                                        from 
                                        (select TimeOfDay,
                                        ViolationCode,
                                        count(*) as TicketCount
                                        from violations_time_bin 
                                        group by TimeOfDay,
                                        ViolationCode))
                                        where Rank <= 3")
  head(tickets_by_time_of_day,nrow(tickets_by_time_of_day))
  # Spark Definition - dense_rank() - Computes the rank of a value in a group of values. The result is one plus the previously assigned rank value. Unlike the function rank, dense_rank will not produce gaps in the ranking sequence.
  # dense_rank & filtering the rank <=3 will ensure we get the top 3 in each violation code & time of day. 
  
  #TimeOfDay ViolationCode TicketCount Rank                                     
  # Midnight            21       61450    1
  # Midnight            40       48573    2
  # Midnight            14       39160    3
  #  Evening            38      283566    1
  #  Evening            37      209979    2
  #  Evening            14      191542    3
  #     Dawn            40       89034    1
  #     Dawn            14       56323    2
  #     Dawn            20       47164    3
  #  Morning            21      961228    1
  #  Morning            36      577626    2
  #  Morning            14      308070    3
  #Afternoon            36      740815    1
  #Afternoon            21      479534    2
  #Afternoon            38      463096    3
  #    Night             7       78550    1
  #    Night            38       47774    2
  #    Night            40       40132    3
  
  # converting to data frame to do a ggplot. 
  df_tickets_by_time_of_day <- data.frame(head(tickets_by_time_of_day,nrow(tickets_by_time_of_day)))
  ggplot(df_tickets_by_time_of_day, aes(x= as.factor(ViolationCode), y=TicketCount)) + geom_col()+ facet_grid(~TimeOfDay)  + xlab("Violation Code") + ylab("Ticket Count") + ggtitle("Ticket count by time of day & violation") + geom_text(aes(label=TicketCount))
  
  
  #### 5.4 Now, try another direction. For the three most commonly occurring violation codes, find the most common time of the day (in terms of the bins from the previous part)
  top_3_violations <- SparkR::sql("select ViolationCode,count(*) as TicketCount from violations_time_bin group by ViolationCode order by count(*) desc Limit 3")
  head(top_3_violations)
  
  #ViolationCode TicketCount                                                     
  #           21     1528545
  #           36     1400614
  #           38     1062301
  
  # Creating a temporary view for accessing using SQL. 
  createOrReplaceTempView(top_3_violations, "top_3_violations")
  top_3_violation_time_bin <- SparkR::sql("select * 
                                          from (
                                          select ViolationCode,TimeOfDay,TicketCount,
                                          dense_rank() over(partition by ViolationCode order by TicketCount desc) as Rank
                                          from (select vt.ViolationCode,vt.TimeOfDay,count(*) as TicketCount
                                          from violations_time_bin vt,
                                          top_3_violations t3
                                          where t3.ViolationCode = vt.ViolationCode
                                          group by vt.ViolationCode,vt.TimeOfDay)
                                          ) where Rank <= 3")
  
  head(top_3_violation_time_bin,nrow(top_3_violation_time_bin))
  
  #ViolationCode TimeOfDay TicketCount Rank                                      
  #           38 Afternoon      463096    1
  #           38   Evening      283566    2
  #           38   Morning      264695    3
  #           21   Morning      961228    1
  #           21 Afternoon      479534    2
  #           21  Midnight       61450    3
  #           36 Afternoon      740815    1
  #           36   Morning      577626    2
  #           36   Evening       81851    3
  
  #-----------------------------------------------------------#
  
  #### 6. Let’s try and find some seasonality in this data
  #### 6.1 First, divide the year into some number of seasons, and find frequencies of tickets for each season. 
  #### Hint: Use Issue Date to segregate into seasons
  
  # We have already extracted the Month, Date & year fields in the dataset. 
  # Confirming that we have only the months numbered between 1 & 12. 
  distinct_violation_month <- SparkR::sql("select IssueMonth,count(*) as count from violations_time group by IssueMonth")
  head(distinct_violation_month,nrow(distinct_violation_month))
  
  # Confirming the structure of violations_time_bin to validate if IssueMonth is set as integer. 
  str(violations_time_bin)
  violations_season_bin <- SparkR::sql("select IssueYear,
                                       IssueMonth,
                                       IssueDt,
                                       SummonsNumber,
                                       PlateID,
                                       RegistrationState,
                                       IssueDate,
                                       ViolationCode,
                                       VehicleBodyType,
                                       VehicleMake,
                                       VehicleMake,
                                       ViolationPrecinct,
                                       IssuerPrecinct,
                                       ViolationTime,
                                       ViolationHour,
                                       ViolationMinute,
                                       ViolationPeriod,
                                       TimeOfDay,
                                       case when (IssueMonth = 12 or IssueMonth = 1 or IssueMonth = 2) then 'Winter'
                                       when (IssueMonth = 3 or IssueMonth = 4 or IssueMonth = 5) then 'Spring'
                                       when (IssueMonth = 6 or IssueMonth = 7 or IssueMonth = 8) then 'Summer'
                                       Else 'Fall'
                                       end as Season
                                       from violations_time_bin")
  
  head(violations_season_bin)
  
  # Creating a temporary view for accessing using SQL. 
  createOrReplaceTempView(violations_season_bin, "violations_season_bin")
  
  #### 6.2 Find the three most common violations for each of these seasons.
  
  violations_by_season <- SparkR::sql("select * 
                                      from (select Season,
                                      ViolationCode,
                                      TicketCount,
                                      dense_rank () over (partition by Season 
                                      order by TicketCount desc) Rank
                                      from (select Season,
                                      ViolationCode,
                                      count(*) as TicketCount
                                      from violations_season_bin 
                                      group by Season,
                                      ViolationCode
                                      )
                                      )
                                      where Rank <= 3")
  head(violations_by_season,nrow(violations_by_season))
  
  #Season ViolationCode TicketCount Rank                                        
  #Spring            21      402790    1
  #Spring            36      344834    2
  #Spring            38      271192    3
  #Summer            21      405944    1
  #Summer            38      247560    2
  #Summer            36      240396    3
  #  Fall            36      456046    1
  #  Fall            21      357475    2
  #  Fall            38      283827    3
  #Winter            21      362336    1
  #Winter            36      359338    2
  #Winter            38      259722    3
  
  # Even here, we are using dense_rank & rank<=3 to get the results. 
  #-----------------------------------------------------------#
  
  #### 7. The fines collected from all the parking violation constitute a revenue source for the NYC police department. Let’s take an example of estimating that for the three most commonly occurring codes.
  #### 7.1  Find total occurrences of the three most common violation codes
  head(top_3_violations)
  # Reusing the same dataframe created in question 5.4
  #ViolationCode TicketCount                                                     
  #           21     1528545
  #           36     1400614
  #           38     1062301
  
  #### 7.2 Visit the nyc.gov website & get the fine amount as mentioned below. 
  # Following are the top 3 violations & their associated fine & description extracted from http://www1.nyc.gov/site/finance/vehicles/services-violation-codes.page
  # It lists the fines associated with different violation codes. They’re divided into two categories, one for the highest-density locations of the city, the other for the rest of the city. 
  # For simplicity, take an average of the two.
  # Code 21    - Max 65- Min 45- Average 55
  #            - Code Desc "Street Cleaning: No parking where parking is not allowed by sign, street marking or traffic control device."
  # Code 36    - Max 50- Min 50- Average 50
  #            - Code Desc "Exceeding the posted speed limit in or near a designated school zone."
  # Code 38    - Max 65- Min 35- Average 50
  #            - Code Desc "Parking Meter -- Failing to show a receipt or tag in the windshield. Drivers get a 5-minute grace period past the expired time on parking meter receipts."
  
  # Creating a temporary view for accessing using SQL and doing a SQL join. 
  createOrReplaceTempView(top_3_violations, "top_3_violations")
  
  # Based on the reference in www.nyc.gov site, we are creating the R dataframe for the violationCode & its average Fine amount derived from Min & Max listed in the site. 
  ViolationCode <- c(21,36,38)
  FineAmount <-  c(55,50,50)
  ViolationFine <- data.frame(cbind(ViolationCode,FineAmount))
  ViolationFine$ViolationCode <- sapply(ViolationFine$ViolationCode,as.integer)
  str(ViolationFine)
  SparkR::str(top_3_violations)
  
  df_ViolationFine <- data.frame(head(top_3_violations,nrow(top_3_violations)))
  
  # Converting to simple R data frame since teh dataset contains only 3 rows with 3 columns. 
  ticket_revenue <- merge(df_ViolationFine, ViolationFine, by = "ViolationCode")
  ticket_revenue$TotalRevenue <- ticket_revenue$TicketCount*ticket_revenue$FineAmount
  
  ticket_revenue
  
        #ViolationCode TicketCount FineAmount TotalRevenue
        #           21     1528545         55     84069975
        #           36     1400614         50     70030700
        #           38     1062301         50     53115050
  
  #### 7.4 What can you intuitively infer from these findings?
     # Ticket revenue for violation code 21 is 84 million dollars. 
     # Ticket revenue for violation code 36 is 70 million dollars.
     # Ticket revenue for violation code 38 is 53 million dollars. 
     # NYC gets millions of dollars of revenue from each parking violation code & there are millions of parking tickets issued in an year.  
  
  #-----------------------------------------------------------# 
  
  # Plots
  
  # Plot tickets by by Violation code & time of day. 
  df_tickets_by_time_of_day <- data.frame(head(tickets_by_time_of_day,nrow(tickets_by_time_of_day)))
  ggplot(df_tickets_by_time_of_day, aes(x= as.factor(ViolationCode), y=TicketCount)) + geom_col()+ facet_grid(~TimeOfDay)  + xlab("Violation Code") + ylab("Ticket Count") + ggtitle("Ticket count by time of day & violation") + geom_text(aes(label=TicketCount))
  
  # Plot Ticket revenue.
  ggplot(ticket_revenue, aes(x= as.factor(ViolationCode), y=TotalRevenue)) + geom_col() + xlab("Violation Code") + ylab("Ticket Revenue") + ggtitle("Ticket Revenue by violation Code") + geom_text(aes(label=TotalRevenue))+ scale_y_continuous(labels = scales::comma)
  
  # Plot tickets by season. 
  df_violations_by_season <- data.frame(head(violations_by_season,nrow(violations_by_season)))
  ggplot(df_violations_by_season, aes(x= as.factor(ViolationCode), y=TicketCount)) + geom_col()+ facet_grid(~Season)  + xlab("Violation Code") + ylab("Ticket Count") + ggtitle("Ticket count by season & violation code") + geom_text(aes(label=TicketCount))+ scale_y_continuous(labels = scales::comma)
  
  
  # Essential commands
  # Closing the SparkR session to free up the memory. 
  sparkR.stop()