package com.lennart.examples;

import org.qcri.rheem.api.FilterDataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.api.JoinDataQuantaBuilder;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.hive.operators.HiveTableSource;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.hive.Hive;

import java.util.Collection;

public class HiveExample {
    public static void main(String[] args) {
        Configuration config = new Configuration();
        config.setProperty("rheem.hive.jdbc.url", "jdbc:hive2://localhost:10000/default");
        config.setProperty("rheem.hive.jdbc.user", "lbhm");
        config.setProperty("rheem.hive.jdbc.password", "");

        RheemContext rheemContext = new RheemContext(config)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Hive.plugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(rheemContext)
                .withJobName("Hive Example")
                .withUdfJarOf(HiveExample.class);

        FilterDataQuantaBuilder<Record> users = planBuilder
                .readTable(new HiveTableSource("u_user", "userid", "age", "gender", "occupation", "zip"))
                .projectRecords(new String[]{"userid", "age", "gender"})
                .filter(t -> true);

        FilterDataQuantaBuilder<Record> data = planBuilder
                .readTable(new HiveTableSource("u_data", "userid", "movieid", "rating", "unixtime"))
                .projectRecords(new String[]{"movieid", "rating"})
                .filter(t -> Double.parseDouble(t.getString(1)) >= 5);

        JoinDataQuantaBuilder<Record, Record, Double> join = data
                .join(t -> t.getDouble(0), users, t -> t.getDouble(0));

        Collection<Tuple2<Record, Record>> output = join.collect();

        for (Tuple2 t : output) {
            System.out.println(t.field0 + " - " + t.field1);
        }
    }
}
