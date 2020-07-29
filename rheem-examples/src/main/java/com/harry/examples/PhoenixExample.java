package com.harry.examples;

import org.qcri.rheem.api.FilterDataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.phoenix.Phoenix;
import org.qcri.rheem.phoenix.operators.PhoenixTableSource;

import java.util.Collection;

public class PhoenixExample {
    public static void main(String[] args) {


        RheemContext context = new RheemContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Phoenix.plugin());

        context.getConfiguration().setProperty("rheem.phoenix.jdbc.url", "jdbc:phoenix:thin:url=http://localhost:8765/hbase;serialization=PROTOBUF");

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("Phoenix example")
                .withUdfJarOf(PhoenixExample.class);

        FilterDataQuantaBuilder<Record> nation_regionkeys = planBuilder
                .readTable(new PhoenixTableSource("\"nation\"", "\"n_regionkey\"", "\"n_name\""))
                .projectRecords(new String[]{"\"n_regionkey\"", "\"n_name\""})
                //.filter(t -> Integer.parseInt(t.getString(0)) == 4);
                .filter(t -> true);
        //.collect();
        FilterDataQuantaBuilder<Record> regions = planBuilder.readTextFile("file:///home/harry/workspace/polydb/src/main/resources/sales/region2.csv")
                .map(t -> {
                    String[] str = t.split(",");
                    Record rec = new Record(str[0], str[1], str[2]);
                    return rec;
                })
                .filter(t -> true);

        Collection<Record> nation_regions = nation_regionkeys
                .join(t -> t.getField(0), regions, t -> t.getField(0))
                .map(t -> new Record(t.field0.getString(1), t.field1.getString(1)))
                .collect();

        for (Record rkey : nation_regions
        ) {
            System.out.println(rkey);
        }
    }
}
