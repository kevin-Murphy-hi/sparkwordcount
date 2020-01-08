package bao;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class wc {
    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setAppName("JavaWc");
        //        .setMaster("local[2]");

        final JavaSparkContext jsc = new JavaSparkContext(conf);
        //读文件，得到一个RDD
        //final JavaRDD<String> lines = jsc.textFile(args[0]);

        final JavaRDD<String> lines = jsc.textFile("/user/spark/a.txt");

        //通过切分字符串，得到单词的集合
        final JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        //把words变成一个元组
        final JavaPairRDD<String,Integer> tuples =words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        //聚合
        final JavaPairRDD<String,Integer> reduced = tuples.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        //单词和它出现的次数做一个颠倒，交换
        final JavaPairRDD<Integer,String> swaped = reduced.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tup) throws Exception {
                return tup.swap();
            }
        });

        //排序
        final JavaPairRDD<Integer,String> sorted =swaped.sortByKey();

        //数据的一个位置交换
        final JavaPairRDD<String,Integer> res = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tup) throws Exception {
                return tup.swap();
            }
        });

        //最后结果写到文件
        // res.saveAsTextFile(args[1]);
       // res.saveAsTextFile("D:\\ceshi\\out");
//        List<Tuple2<String, Integer>> collect = res.collect();
//        for (Tuple2<String, Integer> tupl : collect) {
//            System.out.println("key:"+tupl._1+"  value:"+tupl._2);
//        }
     res.saveAsTextFile("/user/spark/sparkwordcountjieguo");
        jsc.stop();

    }
}

