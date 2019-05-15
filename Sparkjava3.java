import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class Sparkjava3
{
    public static void main(String args[])
    {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sparktraining");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rd1 = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10),5);

        System.out.println("Number of Partition : " + rd1.getNumPartitions());

        Integer foldResult= rd1.fold(10, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i, Integer j) throws Exception {
                return i+j;
            }
        });

        System.out.println(foldResult);

        JavaRDD<Integer> rd2=sc.parallelize(Arrays.asList(1,2,3,5,6),2);
        System.out.println("number of partition : " +rd2.getNumPartitions());
        Integer foldResult1= rd2.fold(2, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i, Integer j) throws Exception {
                return i*j;
            }
        });
        System.out.println(foldResult1);

        System.out.println("Number of Slides using Rdd2");

        JavaPairRDD<Integer,Integer> pairRDD =rd1.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer i) throws Exception {
                return new Tuple2(i,1);
            }
        });
        JavaRDD<String> fileRDD =sc.textFile("E:\\student.txt");

        System.out.println(fileRDD.collect());

        System.out.println();

        System.out.println(fileRDD.count());

        System.out.println();

        System.out.println(pairRDD.collect());

        System.out.println();

        JavaRDD<String> splitFile =fileRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] words= s.split(" ");
                ArrayList<String> strList = new ArrayList<>();
                for (String word : words)
                {
                    strList.add(word);
                }
                return strList.iterator();

            }
        });

        System.out.println();

        System.out.println(splitFile.collect());

        JavaPairRDD<String,String> stringPairRDD = splitFile.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return (new Tuple2(s,"Bharat"));
            }
        });

        JavaPairRDD<Integer,Integer> pairRDD2 = rd1.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer i) throws Exception {
                return new Tuple2(i,1);
            }
        });



        System.out.println();

        System.out.println(stringPairRDD.collect());


        JavaRDD<String> fileRDD1 =sc.textFile("E:\\file1.txt");

        System.out.println();

        System.out.println(fileRDD1.collect());

        System.out.println();

        System.out.println(fileRDD1.count());

        System.out.println();

        System.out.println(pairRDD2);

        System.out.println();

        System.out.println(splitFile.collect());

        JavaPairRDD<String,Integer> stringPairRDD1 = fileRDD1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(":")[0], Integer.parseInt(s.split(":")[1]));
            }
        });



        System.out.println(stringPairRDD1 .collect());


        System.out.println(stringPairRDD1.groupByKey().collect());

        System.out.println();

                JavaPairRDD<String,Integer> rd3=stringPairRDD1.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i, Integer j) throws Exception {
                return i+j;
            }
        });

        System.out.println();

        System.out.println(rd3.collect());

        System.out.println();

        System.out.println(stringPairRDD1.sortByKey(false).collect());

        System.out.println();

        JavaPairRDD<String,Integer> rd4= stringPairRDD1.mapValues(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer i) throws Exception {
                return i+100;
            }

        });

        System.out.println();

        System.out.println(rd4.collect());

        JavaPairRDD<String,Integer> rd5=stringPairRDD1.flatMapValues(new Function<Integer, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> call(Integer i) throws Exception {
                return (Arrays.asList(i,i+10,i+20));
            }
        });

        System.out.println();

        System.out.println(rd5.collect());

        System.out.println();

        System.out.println("Keys: ");

        System.out.println(stringPairRDD1.keys().collect());

        System.out.println();

        System.out.println("values : ");

        System.out.println(stringPairRDD1.values().collect());

    }
}
