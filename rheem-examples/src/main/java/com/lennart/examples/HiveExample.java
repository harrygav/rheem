package com.lennart.examples;

import org.qcri.rheem.api.FilterDataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Record;
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

        FilterDataQuantaBuilder<Record> data = planBuilder
                .readTable(new HiveTableSource("u_data", "userid", "movieid", "rating", "unixtime"))
                .projectRecords(new String[]{"movieid", "rating"})
                .filter(t -> Double.parseDouble(t.getString(1)) > 6);

        Collection<Record> output = data.collect();

        for (Record r : output) {
            System.out.println(r);
        }
    }
}
