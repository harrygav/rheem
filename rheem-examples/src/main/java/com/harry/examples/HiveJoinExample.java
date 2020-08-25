package com.harry.examples;

import org.qcri.rheem.api.FilterDataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.hive.Hive;
import org.qcri.rheem.hive.operators.HiveTableSource;
import org.qcri.rheem.java.Java;

import java.util.Collection;

public class HiveJoinExample {
    public static void main(String[] args) {


        RheemContext context = new RheemContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Hive.plugin());

        context.getConfiguration().setProperty("rheem.hive.jdbc.url", "jdbc:hive2://localhost:10000/default");

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("Hive Join example")
                .withUdfJarOf(HiveJoinExample.class);

        FilterDataQuantaBuilder<Record> nation_regionkeys = planBuilder
                .readTable(new HiveTableSource("u_data", "userid", "rating"))
                .projectRecords(new String[]{"userid", "rating"})
                .filter(t -> Integer.parseInt(t.getString(0)) == 1).withSqlUdf("userid=1");
                //.filter(t -> true);
        //.collect();
        FilterDataQuantaBuilder<Record> regions = planBuilder.readTextFile("file:///home/harry/workspace/polydb/src/main/resources/sales/region2.csv")
                .map(t -> {
                    String[] str = t.split(",");
                    Record rec = new Record(str[0], str[1], str[2]);
                    return rec;
                })
                .filter(t -> true);


        Collection<Record> nation_regions = nation_regionkeys
                .join(t -> t.getField(0).toString(), regions, t -> t.getField(0).toString())
                .map(t -> new Record(t.field0.getString(1), t.field1.getString(1)))
                .collect();


        for (Record rkey : nation_regions
        ) {
            System.out.println(rkey);
        }
    }
}
