package com.lennart.examples;

import org.qcri.rheem.api.FilterDataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.api.JoinDataQuantaBuilder;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.hive.operators.HiveTableSource;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.hive.Hive;

import java.util.Collection;

public class ScalaSimpleHiveJoin {
    public static void main(String[] args) {
        RheemContext rheemContext = new RheemContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Hive.plugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(rheemContext)
                .withJobName("Scala API Hive Example")
                .withUdfJarOf(ScalaSimpleHiveJoin.class);

        FilterDataQuantaBuilder<Record> users = planBuilder
                .readTable(new HiveTableSource("u_user",
                        "u_user.userid", "u_user.age", "u_user.gender", "u_user.occupation", "u_user.zip"))
                .projectRecords(new String[]{"u_user.userid", "u_user.age", "u_user.gender", "u_user.occupation", "u_user.zip"})
                .filter(t -> t.getString(2).equals("M"))
                .withSqlUdf("u_user.gender = 'M'");

        FilterDataQuantaBuilder<Record> data = planBuilder
                .readTable(new HiveTableSource("u_data",
                        "u_data.userid", "u_data.movieid", "u_data.rating", "uu_data.nixtime"))
                .projectRecords(new String[]{"u_data.userid", "u_data.movieid", "u_data.rating"})
                .filter(t -> t.getInt(3) >= 5)
                .withSqlUdf("u_data.rating >= 5");

        JoinDataQuantaBuilder<Record, Record, Double> join = data
                .join(t -> t.getDouble(0), users, t -> t.getDouble(0))
                .withSqlUdf("u_data.userid = u_user.userid");

        Collection<Tuple2<Record, Record>> output = join.collect();

        // The SqlToStreamOperator returns Records even though the JoinOperator is annotated to return a Tuple2.
        // Therefore, we can only access the results as objects and use a type cast.
        for (Object o : output) {
            Record r = (Record) o;
            System.out.println(r);
        }

        // This will not work unless the hack in org.qcri.rheem.api.JoinDataQuantaBuilder.build is fixed.
//        for (Record r : output) {
//            System.out.println(r);
//        }
//        for (Tuple2<Record, Record> t : output) {
//            System.out.println(t.field0 + " - " + t.field1);
//        }
    }
}
