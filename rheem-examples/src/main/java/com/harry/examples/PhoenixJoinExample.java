package com.harry.examples;

import org.qcri.rheem.api.FilterDataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.hbase.operators.HBaseTableSource;
import org.qcri.rheem.hive.Hive;
import org.qcri.rheem.hive.operators.HiveTableSource;
import org.qcri.rheem.java.Java;

import java.util.Collection;

public class PhoenixJoinExample {
    public static void main(String[] args) {


        RheemContext context = new RheemContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Hive.plugin());

        context.getConfiguration().setProperty("rheem.hive.jdbc.url", "jdbc:hive2://localhost:10000/default");
        context.getConfiguration().setProperty("rheem.phoenix.jdbc.url", "jdbc:phoenix:thin:url=http://localhost:8765/hbase;serialization=PROTOBUF");

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("Hive Join example")
                .withUdfJarOf(HiveJoinExample.class);

        FilterDataQuantaBuilder<Record> u_data = planBuilder
                .readTable(new HiveTableSource("u_data", "userid", "rating"))
                .projectRecords(new String[]{"userid", "rating"})
                .filter(t -> true).withSqlUdf("userid=1");
        //.filter(t -> true);
        //.collect();
        FilterDataQuantaBuilder<Record> nation_regionkeys = planBuilder
                .readTable(new HBaseTableSource("\"nation\"", "\"n_regionkey\"", "\"n_name\""))
                .projectRecords(new String[]{"\"n_regionkey\"", "\"n_name\""})
                //.filter(t -> Integer.parseInt(t.getString(0)) == 4);
                .filter(t -> true).withSqlUdf("\"n_regionkey\">0");


        Collection<Record> nation_regions = u_data
                .join(t -> t.getField(0).toString(), nation_regionkeys, t -> t.getField(0).toString()).withTargetPlatform(Java.platform())
                .map(t -> new Record(t.field0.getString(1), t.field1.getString(1)))
                .collect();


        for (Record rkey : nation_regions
        ) {
            System.out.println(rkey);
        }
    }
}
