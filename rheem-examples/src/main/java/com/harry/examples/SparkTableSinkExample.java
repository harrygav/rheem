package com.harry.examples;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.spark.operators.SparkTableSink;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Properties;

public class SparkTableSinkExample {
    public static void main(String[] args) {

        // CSV file.
        Operator csv = new TextFileSource("file:///home/harry/datasets/user/u.user");
        Operator map = new MapOperator<String, Record>(s -> {
            String[] values = s.split(",");
            return new Record(Integer.valueOf(values[0]), Double.valueOf(values[1]));
        }, String.class, Record.class);
        csv.connectTo(0, map, 0);
        map.addTargetPlatform(SparkPlatform.getInstance());

        // Sink connection properties.
        Properties props = new Properties();
        props.setProperty("url", "jdbc:postgres://localhost:5432");
        props.setProperty("database", "default");
        props.setProperty("user", "postgres");
        props.setProperty("password", "123456");
        props.setProperty("driver", "org.postgresql.Driver");

        // Sink.
        Operator sink = new SparkTableSink(props, "spark_tablesink_test", "append");

        // Non-table Operator for testing purposes.
        //Operator sink = new TextFileSink<Record>("/home/lbhm/Desktop/test.txt", Record.class);
        sink.addTargetPlatform(SparkPlatform.getInstance());

        // This does not work because of type mismatch
        //Operator sink = new SparkTableSink(props, "spark_tablesink_test", "append", "userid", "age", "gender", "score");
        map.connectTo(0, sink, 0);

        // Create RheemPlan
        RheemPlan plan = new RheemPlan(sink);

        // Create context and execute.
        RheemContext rheemContext = new RheemContext()
                .withPlugin(Java.basicPlugin())
                //.withPlugin(Hive.plugin())
                .withPlugin(Spark.basicPlugin());

        Job job = rheemContext.createJob("Java API Join Example", plan);
        job.execute();
    }
}
