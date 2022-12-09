package spark.job3;


import org.apache.spark.sql.SparkSession;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class App{
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("job3").setMaster("local");
        try (JavaSparkContext job3 = new JavaSparkContext(conf)) {
            SparkSession spark = SparkSession
                    .builder()
                    .appName("job2")
                    .master("local")
                    .getOrCreate();

            Dataset<Row> df_stock = spark.read().csv("/Users/ff/Desktop/data/stock_data.csv");

            

           
            spark.stop();
            job3.stop();
        }
    }

}
