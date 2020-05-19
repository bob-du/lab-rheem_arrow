package org.qcri.rheem.spark.operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.qcri.rheem.spark.arrow.RddToCollectionByFlight;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author: Du Qinghua
 * @date: 2020/5/19-10:32
 * @file: SparkStreamOperatorTest
 */
public class SparkStreamOperatorTest {
    JavaRDD<?> inputRdd;
    JavaSparkContext context;

    @Before
    public void initRdd() {
        SparkConf conf = new SparkConf()
                .setAppName("test flight")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setMaster("local[*]");
        context = new JavaSparkContext(conf);
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            data.add(i);
        }
        inputRdd = context.parallelize(data, 4);
    }

    /**
     * 测试使用arrow传输
     */
    @Test
    public void test_arrow() {
        List<Object> result = RddToCollectionByFlight.convertRddToCollection(inputRdd);
        System.out.println(result);
    }

    /**
     * 使用spark的collect算子
     */
    @Test
    public void test_collect() {
        List<?> result = inputRdd.collect();
        System.out.println(result);
    }

    // 分别打印出运行时间
    // 对于同样的运算，spark回缓存之前的结果，因此先使用一次action来缓存结果，排除
    private void printRunTime(JavaRDD<?> javaRDD) {
        javaRDD.cache();
        javaRDD.foreachPartition(iterator -> {
            // pass
        });
        long startTime = System.currentTimeMillis();
        javaRDD.collect();
        long endTime = System.currentTimeMillis();
        System.out.println("running time of collect: " + (endTime - startTime) + "ms");
        RddToCollectionByFlight.convertRddToCollection(javaRDD);
        long endTime2 = System.currentTimeMillis();
        System.out.println("running time of arrow flight: " + (endTime2 - endTime) + "ms");
    }

    @Test
    public void test1() {
        // 1000个int
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            data.add(i);
        }
        inputRdd = context.parallelize(data, 4);
        printRunTime(inputRdd);
    }

    /**
     * 1.测试不同的数量的差异
     */
    @Test
    public void testDifferentAmount() {

        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            data.add(i);
        }
        //
        inputRdd = context.parallelize(data, 4);
        printRunTime(inputRdd);

        // 100000
        data = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            data.add(i);
        }
        //
        inputRdd = context.parallelize(data, 4);
        printRunTime(inputRdd);

        // 10000000
        data = new ArrayList<>();
        for (int i = 0; i < 10000000; i++) {
            data.add(i);
        }
        //
        inputRdd = context.parallelize(data, 4);
        printRunTime(inputRdd);
    }

    /**
     * 测试不同的partition
     */
    @Test
    public void testDifferentPartition() {
        // 1000000个int
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            data.add(i);
        }
        // 1个partition
        inputRdd = context.parallelize(data, 1);
        printRunTime(inputRdd);
        // 4个partition
        inputRdd = context.parallelize(data, 4);
        printRunTime(inputRdd);
        // 16个partition
        inputRdd = context.parallelize(data, 16);
        printRunTime(inputRdd);
    }

    /**
     * 测试不同的数据
     */
    @Test
    public void testDifferentType() {
        // 1000000个int
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            data.add(i);
        }
        inputRdd = context.parallelize(data, 4);
        printRunTime(inputRdd);

        // 100000个string
        List<String> stringData = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            stringData.add(UUID.randomUUID().toString());
        }
        inputRdd = context.parallelize(stringData, 4);
        printRunTime(inputRdd);

        // 1000000个double
        List<Double> doubleData = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            doubleData.add(i + i / 100.0);
        }
        inputRdd = context.parallelize(doubleData, 4);
        printRunTime(inputRdd);
    }

    @After
    public void closeSpark() {
        context.close();
    }
}
