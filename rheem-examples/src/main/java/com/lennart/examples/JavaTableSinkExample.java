package com.lennart.examples;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.hive.Hive;
import org.qcri.rheem.hive.operators.HiveTableSource;
import org.qcri.rheem.hive.platform.HivePlatform;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.java.operators.JavaTableSink;

import java.util.Properties;

public class JavaTableSinkExample {
    public static void main(String[] args) {

        // Hive table.
        Operator u_users = new HiveTableSource("u_user");
        Operator projection = MapOperator.createProjection(Record.class, Record.class, "userid", "age", "gender");
        u_users.connectTo(0, projection, 0);

        PredicateDescriptor pred = new PredicateDescriptor(t -> true, Record.class).withSqlImplementation("u_user.age > 25");
        Operator selection = new FilterOperator<Record>(pred);
        selection.addTargetPlatform(HivePlatform.getInstance());
        projection.connectTo(0, selection, 0);

        // CSV file.
        Operator csv = new TextFileSource("file:///home/lbhm/hive/data/csvJoinExample.csv");
        Operator map = new MapOperator<String, Record>(s -> {
            String[] values = s.split(",");
            return new Record(Integer.valueOf(values[0]), Double.valueOf(values[1]));
        }, String.class, Record.class);
        csv.connectTo(0, map, 0);

        // Second join.
        Operator join = new JoinOperator<Record, Record, Integer>(
                r -> r.getInt(0),
                r -> r.getInt(0),
                Record.class, Record.class, Integer.class);
        selection.connectTo(0, join, 0);
        map.connectTo(0, join, 1);

        // Flatten the join tuple.
        Operator map2 = new MapOperator<Tuple2, Record>(t -> {
            Record r0 = (Record) t.getField0();
            Record r1 = (Record) t.getField1();
            return new Record(r0.getInt(0), r0.getInt(1), r0.getString(2), r1.getDouble(1));
        }, Tuple2.class, Record.class);
        join.connectTo(0, map2, 0);

        // Sink connection properties.
        Properties hiveProps = new Properties();
        hiveProps.setProperty("url", "jdbc:hive2://localhost:10000");
        hiveProps.setProperty("database", "default");
        hiveProps.setProperty("user", "lbhm");
        hiveProps.setProperty("driver", "org.apache.hive.jdbc.HiveDriver");

        Properties postgresProps = new Properties();
        postgresProps.setProperty("url", "jdbc:postgresql://localhost/rheemTest");
        postgresProps.setProperty("user", "rheemTest");
        postgresProps.setProperty("driver", "org.postgresql.Driver");

        // Sink.
        Operator sink = new JavaTableSink(postgresProps, "overwrite", "java_tablesink_test");
        map2.connectTo(0, sink, 0);

        // Create RheemPlan
        RheemPlan plan = new RheemPlan(sink);

        // Create context and execute.
        RheemContext rheemContext = new RheemContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Hive.plugin());

        Job job = rheemContext.createJob("JavaTableSink Example", plan);
        job.execute();
    }
}
