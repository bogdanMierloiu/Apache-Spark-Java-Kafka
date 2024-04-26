package com.bogdanmierloiu;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple22;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        List<Double> doubleInputData = new ArrayList<>();
        doubleInputData.add(35.466);
        doubleInputData.add(37.466);
        doubleInputData.add(65.166);
        doubleInputData.add(30.4232);

        List<Integer> integerInputData = new ArrayList<>();
        integerInputData.add(35);
        integerInputData.add(37);
        integerInputData.add(65);
        integerInputData.add(30);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("StartingSpark").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<Double> doubleRdd = sc.parallelize(doubleInputData);
            Double doubleSumResult = doubleRdd.reduce(Double::sum);
            System.out.println("Double result is: " + doubleSumResult);

            JavaRDD<Integer> integerRdd = sc.parallelize(integerInputData);
            integerRdd.map(Math::sqrt).collect().forEach(System.out::println);

            JavaRDD<Double> sqrtRdd = integerRdd.map(Math::sqrt);

            sqrtRdd.collect().forEach(System.out::println);

            JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
            Long count = singleIntegerRdd.reduce(Long::sum);
            System.out.println("Count is: " + count);

            //            Tuples

            JavaRDD<IntegerWithSquareRoot> integerWithSquareRootJavaRDD = integerRdd.map(IntegerWithSquareRoot::new);
            JavaRDD<Tuple2<Integer, Double>> integerWithSquareRootJavaRDDinTuple = integerRdd.map(value -> new Tuple2<>(value, Math.sqrt(value)));

            Tuple2<Integer, Double> tuple2 = new Tuple2<>(9, 3.0);
            Tuple22<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple22Max = new Tuple22<>(9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 9, 8);


            //            Pair RDDs
            System.out.println("---------- Pair RDDs----------");

            List<String> inputStringData = new ArrayList<>();
            inputStringData.add("WARN: Tuesday 4 September 0405");
            inputStringData.add("ERROR: Tuesday 4 September 0408");
            inputStringData.add("FATAL: Wednesday 5 September 1632");
            inputStringData.add("ERROR: Friday 7 September 1854");
            inputStringData.add("WARN: Saturday 8 September 1942");

            JavaRDD<String> originalLogMessages = sc.parallelize(inputStringData);
            JavaPairRDD<String, String> pairRdd = originalLogMessages.mapToPair(rawValue -> {
                String[] columns = rawValue.split(":");
                String level = columns[0];
                String date = columns[1];
                return new Tuple2<>(level, date);
            });
            pairRdd.collect().forEach(System.out::println);


//
//            System.out.println("---------- CSV files ----------");
            JavaRDD<String> stringJavaRDD = sc.textFile("src/main/resources/api-logs/interview-api-25.04.2024.csv");
            analyzeAppLogs(stringJavaRDD);

        }
    }


    private static void analyzeAppLogs(JavaRDD<String> stringJavaRDD) {
        String header = stringJavaRDD.first();
        JavaRDD<String> filteredData = stringJavaRDD.filter(row -> !row.equals(header) && !row.trim().isEmpty());
        JavaRDD<Tuple3<String, String, String>> tuple3JavaRDD = filteredData.map(rawValue -> {
            String[] columns = rawValue.split(",");
            String latency = columns[0];
            String requestMethod = columns[4];
            String rawRequestUrl = columns[6];
            int startIndex = rawRequestUrl.indexOf("/", "https://".length());
            String requestUrl;
            if (startIndex != -1) {
                requestUrl = rawRequestUrl.substring(startIndex);
            } else {
                requestUrl = rawRequestUrl;
            }
            return new Tuple3<>(latency, requestMethod, requestUrl);
        });

        JavaRDD<Tuple3<String, String, String>> tuple3Filtered = tuple3JavaRDD
                .filter(tuple -> !tuple._1().isEmpty() && !tuple._2().isEmpty() && !tuple._3().isEmpty());
        JavaPairRDD<String, Tuple2<Integer, Double>> endpointStats = tuple3Filtered.mapToPair(tuple -> {
            String endpoint = tuple._3();
            double duration = Double.parseDouble(tuple._1().replace("s", ""));
            return new Tuple2<>(endpoint, new Tuple2<>(1, duration));
        });

        JavaPairRDD<String, Tuple2<Integer, Double>> endpointStatsReduced = endpointStats.reduceByKey((x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2()));
        JavaPairRDD<String, Tuple2<Integer, Double>> sortedByNumCalls = endpointStatsReduced.sortByKey();
        sortedByNumCalls.collect().forEach(System.out::println);

        JavaPairRDD<Double, String> sortedByTotalDuration = endpointStatsReduced
                .mapToPair(tuple -> new Tuple2<>(tuple._2()._2(), tuple._1()))
                .sortByKey(false);

//        sortedByTotalDuration.collect().forEach(System.out::println);
    }
}
