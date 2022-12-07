package spark.job2;


import org.apache.spark.sql.SparkSession;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class App{
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("job2").setMaster("local");
        try (JavaSparkContext job2_1 = new JavaSparkContext(conf)) {
            SparkSession spark = SparkSession
                    .builder()
                    .appName("job2_1")
                    .master("local")
                    .getOrCreate();

            Dataset<Row> df_stock = spark.read().csv("/Users/ff/Desktop/data/stock_small.csv");
            Dataset<Row> df_dividends = spark.read().csv("/Users/ff/Desktop/data/dividends_small.csv");

            df_stock.createOrReplaceTempView("stock");
            df_dividends.createOrReplaceTempView("dividends");
            Dataset<Row> temp = spark.sql("SELECT dividends._c2, dividends._c1, stock._c6 FROM stock RIGHT JOIN dividends ON dividends._c1 = stock._c1 AND dividends._c2 = stock._c2 WHERE dividends._c1 = 'IBM'");
            temp.javaRDD().coalesce(1).saveAsTextFile("/Users/ff/Desktop/result/job2_1");

            

            spark.stop();
            job2_1.stop();
        }
    }

}
