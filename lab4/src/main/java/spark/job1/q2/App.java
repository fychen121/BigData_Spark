package spark.job1.q2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class App {
    public static void main( String[] args )
    {
        SparkConf conf = new SparkConf().setAppName("job1").setMaster("local");
        try (JavaSparkContext job1_2 = new JavaSparkContext(conf)) {
            JavaRDD<String> lines = job1_2.textFile("/Users/ff/Desktop/data/stock_small.csv");

            //转换为键值对
            JavaPairRDD<String, Float> KeyValueRDD = lines.mapToPair(new PairFunction<String, String, Float>() {
                @Override
                public Tuple2<String, Float> call(String s) throws Exception {
                    String[] s_list = s.split(",");
                    String key =  s_list[0]+ " " + s_list[1] + " " + s_list[2] + " " + s_list[6] + " " + s_list[3];
                    Float value = Float.parseFloat(s_list[6]) - Float.parseFloat(s_list[3]);
                    Tuple2<String, Float> stock = new Tuple2<>(key, value);
                    return stock;
                }
            });

            JavaPairRDD<String, Float> sortRDD = KeyValueRDD.mapToPair(f -> new Tuple2<Float,String>(f._2,f._1)).sortByKey(false).mapToPair(f -> new Tuple2<String, Float>(f._2,f._1));
            JavaRDD<Tuple2<String, Float>> resultRDD = job1_2.parallelize(sortRDD.take(10));
            resultRDD.saveAsTextFile("/Users/ff/Desktop/result/job1_2");

            job1_2.stop();
        }
    }
}
