package com.bogdanmierloiu;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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


            JavaRDD<Integer> integerRDD = sc.parallelize(integerInputData);
            integerRDD.map(Math::sqrt).collect().forEach(System.out::println);

        }

    }
}
