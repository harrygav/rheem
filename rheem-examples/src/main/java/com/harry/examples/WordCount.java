package com.harry.examples;

import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;
import java.util.Collection;
import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        String inputUrl = "file:" + args[0];

        RheemContext context = new RheemContext(new Configuration())
//                .withPlugin(Java.basicPlugin())
                .withPlugin(Java.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("WordCount")
                .withUdfJarOf(WordCount.class);

        Collection<Tuple2<String, Integer>> words = planBuilder
                .readTextFile(inputUrl)
                .withName("Load file")

                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .withName("Split words")

                .filter(text -> !text.isEmpty())
                .withSelectivity(0.99, 0.99, 0.99)
                .withName("Filter empty words")

                .map(word -> new Tuple2<>(word.toLowerCase(), 1))
                .withName("To lowercase and add counter")

                .reduceByKey(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t1.getField1()))
                .withCardinalityEstimator(new DefaultCardinalityEstimator(0.9, 1, false, in -> Math.round(0.01 * in[0])))
                .withName("Add counters")

                .collect();

        System.out.println(words);
    }
}
