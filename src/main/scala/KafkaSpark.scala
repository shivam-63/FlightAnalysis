import java.lang.ExceptionInInitializerError
import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.util.Random
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Cluster, Host, Metadata, Session}
import com.datastax.spark.connector.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import scala.io._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object KafkaSpark {

  val spark =  SparkSession.builder().master("local").appName("Spark CSV reader").getOrCreate();
  import spark.implicits._

  // connect to Cassandra and make a keyspace and table as explained in the document
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
 // val session = cluster.connect()

  //Finding best time of the day and worst time of the day to travel
  def time_of_day (flightDF:DataFrame)  = {

        //Finding total no of flights with delay less than 30 min in 1 hour range for 24 hours
        val dep_delay_map= flightDF.select("DEP_TIME","DEP_DELAY")
           .filter("DEP_TIME is not null")
           .filter("DEP_DELAY is not null")
           .filter("DEP_DELAY<30")
           .map(x => (if((x.getInt(0).toString.length>=0 && x.getInt(0).toString.length<=2)
             || (x.getInt(0).toString.substring(0,x.getInt(0).toString.length-2).toInt==24)) 0
           else (x.getInt(0).toString.substring(0,x.getInt(0).toString.length-2).toInt),x.getDouble(1)))

        val dep_delay_count= dep_delay_map.select(col("_1").alias("hour"),col("_2"))
          .groupBy(col("hour")).agg(count("_2").alias("dep_delay_count(<_30_min)"))
          .orderBy("hour")

        //Finding total no of flights in 1 hour range for 24 hours
        val total_flights_map = flightDF.select("DEP_TIME")
          .filter("DEP_TIME is not null")
         .map(x => (if((x.getInt(0).toString.length>=0 && x.getInt(0).toString.length<=2)
           || (x.getInt(0).toString.substring(0,x.getInt(0).toString.length-2).toInt==24)) 0
         else (x.getInt(0).toString.substring(0,x.getInt(0).toString.length-2).toInt),1))

        val total_flights_count= total_flights_map.select(col("_1").alias("hour_range"),col("_2"))
          .groupBy("hour_range").agg(count("_2").alias("total_flights"))
          .orderBy("hour_range")

        //Join for total delayed flights and total flights in 1 hour range for 24 hours
        val joinExpression = total_flights_count.col("hour_range")===dep_delay_count.col("hour")
        val joinedDF = total_flights_count.join(dep_delay_count,joinExpression,"left")
        val totalcount_delaycount= joinedDF.select(
          col("hour_range")
          ,col("total_flights")
          ,col("dep_delay_count(<_30_min)"))

        val final_result= totalcount_delaycount.withColumn("total_delayed_flights(>_30_min)", when(col("total_flights").isNull, lit(0))
          .otherwise(col("total_flights")) - when(col("dep_delay_count(<_30_min)").isNull, lit(0))
          .otherwise(col("dep_delay_count(<_30_min)")))
    final_result.orderBy("hour_range").coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/time_of_day.csv")
  }

  //Finding best day of the month and worst day of the month to fly
  def day_of_month (flightDF:DataFrame)  = {

        // Finding number of flights in a day of a month with delay greater than 30 min
         val dep_delay_thirty_plus= flightDF.select("MONTH","DAY_OF_MONTH","DEP_DELAY")
             .filter("DEP_DELAY is not null")
             .filter("DEP_DELAY>30")
             .groupBy("MONTH","DAY_OF_MONTH")
             .agg(count("DEP_DELAY").alias("delayed_flight_count"))
             .orderBy("MONTH","DAY_OF_MONTH")

        //Finding total number fo flight in day of a month
        val total_flights=flightDF.select("MONTH","DAY_OF_MONTH","DEP_TIME")
          .filter("DEP_TIME is not null")
          .groupBy("MONTH","DAY_OF_MONTH")
          .agg(count("DEP_TIME").alias("total_flights"))
          .orderBy("MONTH","DAY_OF_MONTH")

        //Join for total delayed flights and total flights
        val joinedDF = total_flights.join(dep_delay_thirty_plus,Seq("MONTH", "DAY_OF_MONTH"),"left")
        val final_result= joinedDF.withColumn("delay_ratio", when(col("delayed_flight_count").isNull
          , lit(0)).otherwise(col("delayed_flight_count"))/when(col("total_flights").isNull
          , lit(0)).otherwise(col("total_flights")) )
            .orderBy("MONTH","DAY_OF_MONTH")

    //Day of month with highest delay ratio
        val worst_day_of_month_avg=final_result.select("MONTH", "DAY_OF_MONTH","total_flights","delayed_flight_count","delay_ratio")
          .groupBy("MONTH").agg(max("delay_ratio").alias("delay_ratio"))
        val worst_day_month=final_result.join(worst_day_of_month_avg,Seq("MONTH","delay_ratio")).orderBy("MONTH")
        worst_day_month.show()
    worst_day_month.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/worst_day_of_month.csv")


    //Day of month with the minimum delay ratio
        val best_day_of_month_avg=final_result.select("MONTH", "DAY_OF_MONTH","total_flights","delayed_flight_count","delay_ratio")
          .groupBy("MONTH").agg(min("delay_ratio").alias("delay_ratio"))
        val best_day_month=final_result.join(best_day_of_month_avg,Seq("MONTH","delay_ratio")).orderBy("MONTH")
        best_day_month.coalesce(1).write.format("com.databricks.spark.csv")
          .option("header", "true").csv("src/main/results/best_day_of_month.csv")
  }

  //Finding best day of the week and worst day of the week to fly
  def day_of_week (flightDF:DataFrame)  = {

        // Finding number of flights in a day of a month with delay greater than 30 min
        val dep_delay_week= flightDF.select("DAY_OF_WEEK","DEP_DELAY")
          .filter("DEP_DELAY is not null")
          .filter("DEP_DELAY>30")
          .groupBy("DAY_OF_WEEK")
          .agg(count("DEP_DELAY").alias("delayed_flight_count"))
          .orderBy("DAY_OF_WEEK")

        //Finding total number fo flight in day of a month
        val total_flights_week=flightDF.select("DAY_OF_WEEK","DEP_TIME")
          .filter("DEP_TIME is not null")
          .groupBy("DAY_OF_WEEK")
          .agg(count("DEP_TIME").alias("total_flights_week"))
          .orderBy("DAY_OF_WEEK")

        //Join for total delayed flights and total flights
        val joinedDF_week = total_flights_week.join(dep_delay_week,Seq("DAY_OF_WEEK"),"left")
        val final_result_week= joinedDF_week.withColumn("delay_ratio", when(col("delayed_flight_count").isNull
          , lit(0)).otherwise(col("delayed_flight_count"))/when(col("total_flights_week").isNull
          , lit(0)).otherwise(col("total_flights_week")) )
          .orderBy("DAY_OF_WEEK")

        val day_of_week_avg=final_result_week.select("DAY_OF_WEEK","total_flights_week","delayed_flight_count","delay_ratio")
          .groupBy("DAY_OF_WEEK").agg(max("delay_ratio").alias("delay_ratio"))
        val day_week=final_result_week.join(day_of_week_avg,Seq("DAY_OF_WEEK","delay_ratio")).orderBy("DAY_OF_WEEK")
    day_week.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/day_week.csv")
  }

  // Best and Worst Month of the year to fly
  def month_of_year (flightDF:DataFrame)  = {


        // Finding number of flights in a day of a month with delay greater than 30 min
        val dep_delay_year= flightDF.select("YEAR","MONTH","DEP_DELAY")
          .filter("DEP_DELAY is not null")
          .filter("DEP_DELAY>30")
          .groupBy("YEAR","MONTH")
          .agg(count("DEP_DELAY").alias("delayed_flight_count"))
          .orderBy("YEAR","MONTH")

        //Finding total number fo flight in day of a month
        val total_flights_year=flightDF.select("YEAR","MONTH","DEP_TIME")
          .filter("DEP_TIME is not null")
          .groupBy("YEAR","MONTH")
          .agg(count("DEP_TIME").alias("total_flights"))
          .orderBy("YEAR","MONTH")

        //Join for total delayed flights and total flights
        val joinedDF_year = total_flights_year.join(dep_delay_year,Seq("YEAR", "MONTH"),"left")
        val final_result_year= joinedDF_year.withColumn("delay_ratio", when(col("delayed_flight_count").isNull
          , lit(0)).otherwise(col("delayed_flight_count"))/when(col("total_flights").isNull
          , lit(0)).otherwise(col("total_flights")) )
          .orderBy("YEAR","MONTH")

        //Finding the worst month of the year for each year
        val worst_month_of_year_avg=final_result_year.select("YEAR", "MONTH","total_flights","delayed_flight_count","delay_ratio")
          .groupBy("YEAR").agg(max("delay_ratio").alias("delay_ratio"))
        val worst_month_year=final_result_year.join(worst_month_of_year_avg,Seq("YEAR","delay_ratio")).orderBy("YEAR")
    worst_month_year.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/worst_month_of_year.csv")

        //Finding the best month of the year for each year
        val best_month_of_year_avg=final_result_year.select("YEAR", "MONTH","total_flights","delayed_flight_count","delay_ratio")
          .groupBy("YEAR").agg(min("delay_ratio").alias("delay_ratio"))
        val best_month_year=final_result_year.join(best_month_of_year_avg,Seq("YEAR","delay_ratio")).orderBy("YEAR")
    best_month_year.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/best_month_of_year.csv")
  }

  //Finding the departure performance of each airport for each year
  def dep_performance_airport (flightDF:DataFrame)  = {

    //reading airport data from the csv file
    val airportDF = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("src/main/resources/AIRPORT_ID_NAME.csv")


    //Total no of flights with dep delay greater than 30 min with respect to each airport per year
        val dep_delay_airport= flightDF.select("YEAR","ORIGIN_AIRPORT_ID","DEP_DELAY")
          .filter("DEP_DELAY is not null")
          .filter("DEP_DELAY>30")
          .groupBy("YEAR","ORIGIN_AIRPORT_ID")
          .agg(count("DEP_DELAY").alias("delayed_flight_count"))
          .orderBy("YEAR","ORIGIN_AIRPORT_ID")

        //Finding total number fo flight from a airport per year
        val total_flights_airport=flightDF.select("YEAR","ORIGIN_AIRPORT_ID","DEP_TIME")
          .filter("DEP_TIME is not null")
          .groupBy("YEAR","ORIGIN_AIRPORT_ID")
          .agg(count("DEP_TIME").alias("total_flights"))
          .orderBy("YEAR","ORIGIN_AIRPORT_ID")

        //Join for total delayed flights and total flights from an airport
        val joinedDF_airport = total_flights_airport.join(dep_delay_airport,Seq("YEAR", "ORIGIN_AIRPORT_ID"),"left")
        val final_result_airport= joinedDF_airport.withColumn("delay_ratio", when(col("delayed_flight_count").isNull
          , lit(0)).otherwise(col("delayed_flight_count"))/when(col("total_flights").isNull
          , lit(0)).otherwise(col("total_flights")) )
          .orderBy("YEAR","ORIGIN_AIRPORT_ID")

        //Finding the airports with highest departure delay ratio for each year
        val worst_airport_dep_of_year_avg=final_result_airport.select("YEAR", "ORIGIN_AIRPORT_ID","total_flights","delayed_flight_count","delay_ratio")
          .groupBy("YEAR").agg(max("delay_ratio").alias("delay_ratio"))
        val worst_airport_dep_year=final_result_airport.join(worst_airport_dep_of_year_avg,Seq("YEAR","delay_ratio")).orderBy("YEAR")
        val join_airport_name_dep_worst=worst_airport_dep_year.join(airportDF,worst_airport_dep_year("ORIGIN_AIRPORT_ID")===airportDF("AIRPORT_ID"),"inner").drop(airportDF("AIRPORT_ID"))
    join_airport_name_dep_worst.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/dep_performance_airport_worst.csv")

        //Finding the airports with least departure delay ratio for each year
        val best_airport_dep_of_year_avg=final_result_airport.select("YEAR", "ORIGIN_AIRPORT_ID","total_flights","delayed_flight_count","delay_ratio")
          .groupBy("YEAR").agg(min("delay_ratio").alias("delay_ratio"))
        val best_airport_dep_year=final_result_airport.join(best_airport_dep_of_year_avg,Seq("YEAR","delay_ratio")).orderBy("YEAR")
        val join_airport_name_dep_best=best_airport_dep_year.join(airportDF,best_airport_dep_year("ORIGIN_AIRPORT_ID")===airportDF("AIRPORT_ID"),"inner").drop(airportDF("AIRPORT_ID"))
    join_airport_name_dep_best.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/dep_performance_airport_best.csv")
  }


  //Finding the arrival performance of each airport for each year
  def arr_performance_airport (flightDF:DataFrame)  = {

    //reading airport data from the csv file
    val airportDF = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("src/main/resources/AIRPORT_ID_NAME.csv")

    //Total no of flights with arrival delay greater than 30 min with respect to each airport per year
    val dep_delay_arr_airport= flightDF.select("YEAR","DEST_AIRPORT_ID","ARR_DELAY")
      .filter("ARR_DELAY is not null")
      .filter("ARR_DELAY>30")
      .groupBy("YEAR","DEST_AIRPORT_ID")
      .agg(count("ARR_DELAY").alias("delayed_flight_count"))
      .orderBy("YEAR","DEST_AIRPORT_ID")

    //Finding total number of flight to an airport per year
    val total_arr_flights_airport=flightDF.select("YEAR","DEST_AIRPORT_ID","ARR_TIME")
      .filter("ARR_TIME is not null")
      .groupBy("YEAR","DEST_AIRPORT_ID")
      .agg(count("ARR_TIME").alias("total_flights"))
      .orderBy("YEAR","DEST_AIRPORT_ID")

    //Join for total delayed flights and total flights from to an airport
    val joinedDF_airport_arr = total_arr_flights_airport.join(dep_delay_arr_airport,Seq("YEAR", "DEST_AIRPORT_ID"),"left")
    val final_result_arr_airport= joinedDF_airport_arr.withColumn("delay_ratio", when(col("delayed_flight_count").isNull
      , lit(0)).otherwise(col("delayed_flight_count"))/when(col("total_flights").isNull
      , lit(0)).otherwise(col("total_flights")) )
      .orderBy("YEAR","DEST_AIRPORT_ID")

    //Finding the airports with highest arrival delay ratio for each year
    val worst_airport_arr_of_year_avg=final_result_arr_airport.select("YEAR", "DEST_AIRPORT_ID","total_flights","delayed_flight_count","delay_ratio")
      .groupBy("YEAR").agg(max("delay_ratio").alias("delay_ratio"))
    val worst_airport_arr_year=final_result_arr_airport.join(worst_airport_arr_of_year_avg,Seq("YEAR","delay_ratio")).orderBy("YEAR")
    val join_airport_name_arr_worst=worst_airport_arr_year.join(airportDF,worst_airport_arr_year("DEST_AIRPORT_ID")===airportDF("AIRPORT_ID"),"inner").drop(airportDF("AIRPORT_ID"))
    join_airport_name_arr_worst.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/arr_performance_airport_worst.csv")

    //Finding the airports with least arrival delays ratio for each year
    val best_airport_arr_of_year_avg=final_result_arr_airport.select("YEAR", "DEST_AIRPORT_ID","total_flights","delayed_flight_count","delay_ratio")
      .groupBy("YEAR").agg(min("delay_ratio").alias("delay_ratio"))
    val best_airport_arr_year=final_result_arr_airport.join(best_airport_arr_of_year_avg,Seq("YEAR","delay_ratio")).orderBy("YEAR")
    val join_airport_name_arr_best=best_airport_arr_year.join(airportDF,best_airport_arr_year("DEST_AIRPORT_ID")===airportDF("AIRPORT_ID"),"inner").drop(airportDF("AIRPORT_ID"))
    join_airport_name_arr_best.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/arr_performance_airport_best.csv")
  }


  //Finding the airline arrival performance for for each year
  def arr_performance_airline (flightDF:DataFrame)  = {

    //reading flight data from the csv file
    val airlineDF = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("src/main/resources/L_UNIQUE_CARRIERS.csv")

        //Finding total number of flight by an airline per year
        val total_flights_airline=flightDF.select("YEAR","OP_UNIQUE_CARRIER","ARR_TIME")
          .filter("ARR_TIME is not null")
          .groupBy("YEAR","OP_UNIQUE_CARRIER")
          .agg(count("ARR_TIME").alias("total_flights"))
          .orderBy("YEAR","OP_UNIQUE_CARRIER")

        //Total no of flights with arrival delay greater than 30 min with respect to each airline per year
        val arr_delay_airline= flightDF.select("YEAR","OP_UNIQUE_CARRIER","ARR_DELAY")
          .filter("ARR_DELAY is not null")
          .filter("ARR_DELAY>30")
          .groupBy("YEAR","OP_UNIQUE_CARRIER")
          .agg(count("ARR_DELAY").alias("delayed_flight_count"))
          .orderBy("YEAR","OP_UNIQUE_CARRIER")

        //Join for total delayed flights and total flights from to an airport
        val joinedDF_airline_arr = total_flights_airline.join(arr_delay_airline,Seq("YEAR", "OP_UNIQUE_CARRIER"),"left")
        val final_result_arr_airline= joinedDF_airline_arr.withColumn("delay_ratio", when(col("delayed_flight_count").isNull
          , lit(0)).otherwise(col("delayed_flight_count"))/when(col("total_flights").isNull
          , lit(0)).otherwise(col("total_flights")) )
          .orderBy("YEAR","OP_UNIQUE_CARRIER")

        //Finding the airlines with highest arrival delay ratio for each year
        val worst_airline_arr_of_year_avg=final_result_arr_airline.select("YEAR", "OP_UNIQUE_CARRIER","total_flights","delayed_flight_count","delay_ratio")
          .groupBy("YEAR").agg(max("delay_ratio").alias("delay_ratio"))
        val worst_airline_arr_year=final_result_arr_airline.join(worst_airline_arr_of_year_avg,Seq("YEAR","delay_ratio")).orderBy("YEAR")
    val join_with_airline_name_arr_worst=worst_airline_arr_year.join(airlineDF,Seq("OP_UNIQUE_CARRIER"),"inner")
    join_with_airline_name_arr_worst.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/arr_performance_airline_worst.csv")

        //Finding the airlines with least arrival delay ratio for each year
        val best_airline_arr_of_year_avg=final_result_arr_airline.select("YEAR", "OP_UNIQUE_CARRIER","total_flights","delayed_flight_count","delay_ratio")
          .groupBy("YEAR").agg(min("delay_ratio").alias("delay_ratio"))
        val best_airline_arr_year=final_result_arr_airline.join(best_airline_arr_of_year_avg,Seq("YEAR","delay_ratio")).orderBy("YEAR")
    val join_with_airline_name_arr_best=best_airline_arr_year.join(airlineDF,Seq("OP_UNIQUE_CARRIER"),"inner")
    join_with_airline_name_arr_best.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/arr_performance_airline_best.csv")
  }


  //Finding the airline dep performance for for each year
  def dep_performance_airline (flightDF:DataFrame)  = {

    //reading flight data from the csv file
    val airlineDF = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("src/main/resources/L_UNIQUE_CARRIERS.csv")

        //Finding total number of flight by an airline per year
        val total_flights_airline=flightDF.select("YEAR","OP_UNIQUE_CARRIER","DEP_TIME")
          .filter("DEP_TIME is not null")
          .groupBy("YEAR","OP_UNIQUE_CARRIER")
          .agg(count("DEP_TIME").alias("total_flights"))
          .orderBy("YEAR","OP_UNIQUE_CARRIER")

        //Total no of flights with departure delay greater than 30 min with respect to each airline per year
        val dep_delay_airline= flightDF.select("YEAR","OP_UNIQUE_CARRIER","DEP_DELAY")
          .filter("DEP_DELAY is not null")
          .filter("DEP_DELAY>30")
          .groupBy("YEAR","OP_UNIQUE_CARRIER")
          .agg(count("DEP_DELAY").alias("delayed_flight_count"))
          .orderBy("YEAR","OP_UNIQUE_CARRIER")

        //Join for total delayed flights and total flights from by an airline
        val joinedDF_airline_dep = total_flights_airline.join(dep_delay_airline,Seq("YEAR", "OP_UNIQUE_CARRIER"),"left")
        val final_result_arr_airline= joinedDF_airline_dep.withColumn("delay_ratio", when(col("delayed_flight_count").isNull
          , lit(0)).otherwise(col("delayed_flight_count"))/when(col("total_flights").isNull
          , lit(0)).otherwise(col("total_flights")) )
          .orderBy("YEAR","OP_UNIQUE_CARRIER")

        //Finding the airlines with most arrival delays for each year
        val worst_airline_dep_of_year_avg=final_result_arr_airline.select("YEAR", "OP_UNIQUE_CARRIER","total_flights","delayed_flight_count","delay_ratio")
          .groupBy("YEAR").agg(max("delay_ratio").alias("delay_ratio"))
        val worst_airline_dep_year=final_result_arr_airline.join(worst_airline_dep_of_year_avg,Seq("YEAR","delay_ratio")).orderBy("YEAR")
        val join_with_airline_name_dep_worst=worst_airline_dep_year.join(airlineDF,Seq("OP_UNIQUE_CARRIER"),"inner")

    join_with_airline_name_dep_worst.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/dep_performance_worst.csv")

        //Finding the best airlines with least arrival delay ratio for each year
        val best_airline_dep_of_year_avg=final_result_arr_airline.select("YEAR", "OP_UNIQUE_CARRIER","total_flights","delayed_flight_count","delay_ratio")
          .groupBy("YEAR").agg(min("delay_ratio").alias("delay_ratio"))
        val best_airport_dep_year=final_result_arr_airline.join(best_airline_dep_of_year_avg,Seq("YEAR","delay_ratio")).orderBy("YEAR")
        val join_with_airline_name_dep_best=best_airport_dep_year.join(airlineDF,Seq("OP_UNIQUE_CARRIER"),"inner")
    join_with_airline_name_dep_best.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv("src/main/results/dep_performance_best.csv")
  }


  def main(args: Array[String]) {

//    val spark =  SparkSession.builder().master("local").appName("Spark CSV reader").getOrCreate();
//    import spark.implicits._

//creating cassandra keyspace
 //   session.execute("CREATE KEYSPACE IF NOT EXISTS flight_analysis WITH REPLICATION ={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")

    // Create a local StreamingContext with two working threads and batch interval of 1 second.
//    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
//    val ssc = new StreamingContext(conf, Seconds(1))
//    val sc= new SparkContext(conf)
//    ssc.checkpoint(".")

//    // make a connection to Kafka and read (key, value) pairs from it
//    val kafkaConf = Map(
//      "metadata.broker.list" -> "localhost:9092",
//      "zookeeper.connect" -> "localhost:2181",
//      "group.id" -> "kafka-spark-streaming",
//      "zookeeper.connection.timeout.ms" -> "1000")
//    val topics = Set("avg")
//    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)
//
//
    //reading flight data from the csv file
      val flightDF = spark.read
        .format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true") //first line in file has headers
        .option("mode", "DROPMALFORMED")
        .load("src/main/resources/output.csv")


    time_of_day(flightDF)
    day_of_week(flightDF)
    day_of_month(flightDF)
    month_of_year(flightDF)
    dep_performance_airline(flightDF)
    dep_performance_airport(flightDF)
    arr_performance_airline(flightDF)
    arr_performance_airport(flightDF)




//     store the result in Cassandra
    //result.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

//    ssc.start()
//    ssc.awaitTermination()
  }
}