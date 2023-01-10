package spark.job1.q1;


import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class App 
{
    public static void main( String[] args )
    {
        SparkConf conf = new SparkConf().setAppName("job1").setMaster("local");
        try (JavaSparkContext job1_1 = new JavaSparkContext(conf)) {
            JavaRDD<String> lines = job1_1.textFile("/Users/ff/Desktop/data/stock_small.csv");

            //转换为键值对
            JavaPairRDD<String, Long> KeyValueRdd = lines.mapToPair(new PairFunction<String, String, Long>() {
                @Override
                public Tuple2<String, Long> call(String s) throws Exception {
                    String[] s_list = s.split(",");
                    String key =  s_list[2].substring(0,4) + " " + s_list[1];
                    Long value = Long.parseLong(s_list[7]);
                    Tuple2<String, Long> stock = new Tuple2<>(key, value);
                    return stock;
                }
            });

            JavaPairRDD<String, Long> countRDD = KeyValueRdd.reduceByKey(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long Long, Long Long2) throws Exception {
                    return Long + Long2;
                }
            });

            JavaPairRDD<String, Long> sortRDD = countRDD.mapToPair(f -> new Tuple2<Long,String>(f._2,f._1)).sortByKey(false).mapToPair(f -> new Tuple2<String, Long>(f._2,f._1));

            JavaPairRDD<String, String> newKeyValueRDD = sortRDD.mapToPair(new PairFunction<Tuple2<String, Long>, String, String>() {

                @Override
                public Tuple2<String, String> call(Tuple2<String, Long> t) throws Exception {
                    String new_key = t._1.substring(0,4);
                    String new_value = t._1.substring(5) + " " + Long.toString(t._2());
                    Tuple2<String, String> result = new Tuple2<>(new_key, new_value);
                    return result;
                }
            });

            JavaPairRDD<String, String> resultRDD = newKeyValueRDD.partitionBy(new Partitioner(){

                @Override
                public int getPartition(Object key) {
                    return (Integer.parseInt((String) key) + 2) % numPartitions();
                }

                @Override
                public int numPartitions() {
                    return 11;
                }
                
            });

            //List<Tuple2<String, String>> output = resultRDD.collect();
            // Iterator<Tuple2<String, String>> iter = resultRDD.collect().iterator();
            // while(iter.hasNext()){
            //     Tuple2<String, String> result = iter.next();
            //     System.out.println(result._1 + " " + result._2);
            // }
            resultRDD.saveAsTextFile("/Users/ff/Desktop/result/job1_1");

            job1_1.stop();
        }
    }
}
