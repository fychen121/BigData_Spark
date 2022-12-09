package spark.job3;


import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class App{
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("job3").setMaster("local");
        try (JavaSparkContext job3 = new JavaSparkContext(conf)) { 
       
            JavaRDD<String> lines = job3.textFile("/Users/ff/Desktop/data/stock_data.csv");
            List<String> codes = job3.textFile("/Users/ff/Desktop/job3_2/part-00000").collect();
            JavaRDD<LabeledPoint> data = lines.map(new Function<String, LabeledPoint>() {
                private static final long serialVersionUID = 1L;
                @Override
                public LabeledPoint call(String str) throws Exception {
                    
                    String[] t1 = str.split(",");
                    Double t2;
                    if (t1[0] == "NYSE")
                        t2 = 0.0;
                    else
                        t2 = 1.0;
                    // 交易所只有nyse和nasdaq
                    Double t3 = (double) codes.indexOf("[" + t1[1] + "]");
                    LabeledPoint LP = new LabeledPoint(Double.parseDouble(t1[7]),
                            Vectors.dense(t2, t3, Double.parseDouble(t1[2].substring(0,4)), Double.parseDouble(t1[2].substring(5,7)), Double.parseDouble(t1[3]), Double.parseDouble(t1[4]), Double.parseDouble(t1[5]), Double.parseDouble(t1[6])));
                    return LP;
                }
            });
            // Split data into training (80%) and test (20%).
            
            JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] { 0.8, 0.2 }, 11L);
            JavaRDD<LabeledPoint> traindata = splits[0];
		    JavaRDD<LabeledPoint> testdata = splits[1];
            // testdata.coalesce(1).saveAsTextFile("/Users/ff/Desktop/data/test_data");

            final NaiveBayesModel model = NaiveBayes.train(traindata.rdd(), 1.0, "multinomial");
            //测试model，若未切分数据，可免去。
            JavaPairRDD<Double, Double> predictionAndLabel = testdata.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Tuple2<Double, Double> call(LabeledPoint p) {
                    return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                }
            });
            //由测试数据得到模型分类精度
            double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Boolean call(Tuple2<Double, Double> pl) {
                    return pl._1().equals(pl._2());
                }
            }).count() / (double) testdata.count();
            
            System.out.println("模型精度为：" + accuracy);


            JavaRDD<String> testData = job3.textFile("/Users/ff/Desktop/data/test_data/part-00000");
            //(0.0,[1.0,251.0,2006.0,5.0,14.01,14.2,14.01,18700.0])

            JavaPairRDD<String, String> res = testData.mapToPair(new PairFunction<String, String, String>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Tuple2<String,String> call(String line) throws Exception{
                    String[] t1 = line.substring(1, line.length()-1).split(",");
                    Vector v = Vectors.dense(Double.parseDouble(t1[1].substring((1))), Double.parseDouble(t1[2]),Double.parseDouble(t1[3]),Double.parseDouble(t1[4]),
                        Double.parseDouble(t1[5]),Double.parseDouble(t1[6]),Double.parseDouble(t1[7]),Double.parseDouble(t1[8].substring(0,t1[8].length()-1)));
                    double res = model.predict(v);
                    String t2,t3;
                    if (Double.parseDouble(t1[1].substring(1)) == 0)
                        t2 = "NYSE";
                    else
                        t2 = "NASDAQ";
                    // 交易所只有nyse和nasdaq
                    t3 = codes.get((int) Double.parseDouble(t1[2]));
                    t3 = t3.substring(1, t3.length()-1);
                    return new Tuple2<String,String>("真实情况:"+t1[0].substring(0,1) 
                        +"给定数据:"+t2+","+t3+","+t1[3].substring(0,4)+"-"+t1[4].substring(0,t1[4].length()-2)+","+t1[5]+","+t1[6]+","+t1[7]+","+t1[8].substring (0, t1[8].length()-3),
                        "预测结果:" + (int)res);
                }
            });
            res.saveAsTextFile("/Users/ff/Desktop/temp");
    
            job3.stop();
        }
    }

}
