package com.lennart.examples;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.hive.Hive;
import org.qcri.rheem.hive.operators.HiveTableSource;
import org.qcri.rheem.java.Java;

public class JavaThreeWayHiveJoin {
    public static void main(String[] args) {
        // Define operators for first table.
        Operator u_data = new HiveTableSource("u_data");
        Operator projection = MapOperator.createProjection(Record.class, Record.class,
                "u_data.userid", "u_data.movieid", "u_data.rating");
        u_data.connectTo(0, projection, 0);

        PredicateDescriptor pred = new PredicateDescriptor(t -> true, Record.class).withSqlImplementation("u_data.rating >= 4");
        Operator selection = new FilterOperator<Record>(pred);
        projection.connectTo(0, selection, 0);

        // Second table.
        Operator u_users = new HiveTableSource("u_user");
        Operator projection2 = MapOperator.createProjection(Record.class, Record.class,
                "u_user.userid", "u_user.age", "u_user.gender");
        u_users.connectTo(0, projection2, 0);

        PredicateDescriptor pred2 = new PredicateDescriptor(t -> true, Record.class).withSqlImplementation("u_user.age > 25");
        Operator selection2 = new FilterOperator<Record>(pred2);
        projection2.connectTo(0, selection2, 0);

        // First JDBC join.
        Operator join = new JoinOperator<Record, Record, Integer>(
                r -> r.getInt(0),
                r -> r.getInt(0),
                Record.class, Record.class, Integer.class)
                .withSqlImplementation("u_data.userid = u_user.userid");
        selection.connectTo(0, join, 0);
        selection2.connectTo(0, join, 1);

        // Third table.
        Operator u_item = new HiveTableSource("u_item");
        Operator projection3 = MapOperator.createProjection(Record.class, Record.class,
                "u_item.movied", "u_item.title", "u_item.release");
        u_item.connectTo(0, projection3, 0);

        PredicateDescriptor pred3 = new PredicateDescriptor(t -> true, Record.class).withSqlImplementation("");
        Operator selection3 = new FilterOperator<Record>(pred3);
        projection3.connectTo(0, selection3, 0);

        // Second JDBC join.
        Operator join2 = new JoinOperator<Record, Record, Integer>(
                r -> r.getInt(0),
                r -> r.getInt(1),
                Record.class, Record.class, Integer.class)
                .withSqlImplementation("u_data.movied = u_item.movieid");
        join.connectTo(0, join2, 0);
        selection3.connectTo(0, join2, 1);

        PredicateDescriptor fakePred = new PredicateDescriptor(t -> true, Tuple2.class).withSqlImplementation("user = 25");
        Operator fakeSelection = new FilterOperator<Record>(fakePred);
        join.connectTo(0, fakeSelection, 0);

        // Flatten the join tuple.
        Operator map = new MapOperator<Tuple2, Record>(t -> {
             Record r = (Record) t.getField0();
             Record r1 = (Record) t.getField1();
            // r0.getInt(0), r1.getInt(1), r1.getString(2), r0.getInt(1), r0.getInt(2)
            return new Record(r.getInt(0), r.getInt(4), r.getString(5), r.getInt(1), r.getInt(2));
        }, Tuple2.class, Record.class);
        fakeSelection.connectTo(0, map ,0);

        // CSV file.
        Operator csv = new TextFileSource("file:///home/lbhm/hive/data/csvJoinExample.csv");
        Operator map2 = new MapOperator<String, Record>(s -> {
            String[] values = s.split(",");
            return new Record(Integer.valueOf(values[0]), Double.valueOf(values[1]));
        }, String.class, Record.class);
        csv.connectTo(0, map2, 0);

        // Java join.
        Operator join3 = new JoinOperator<Record, Record, Integer>(
                r -> r.getInt(0),
                r -> r.getInt(0),
                Record.class, Record.class, Integer.class);
        map.connectTo(0, join3, 0);
        map2.connectTo(0, join3, 1);

        // Flatten the join tuple.
        Operator map3 = new MapOperator<Tuple2, Record>(t -> {
            Record r0 = (Record) t.getField0();
            Record r1 = (Record) t.getField1();
            return new Record(r0.getInt(0), r0.getInt(1), r0.getString(2), r0.getInt(1), r0.getInt(2), r1.getDouble(1));
        }, Tuple2.class, Record.class);
        join2.connectTo(0, map3, 0);

        // Sink.
//        Operator sink = LocalCallbackSink.createStdoutSink(Tuple2.class);
        Operator sink = new TextFileSink<>("file:///home/lbhm/hive/jointest.csv", Tuple2.class);
        join.connectTo(0, sink, 0);

        // Create RheemPlan
        RheemPlan plan = new RheemPlan(sink);

        // Create context and execute.
        RheemContext rheemContext = new RheemContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Hive.plugin());

        Job job = rheemContext.createJob("Java API Join Example", plan);
        job.execute();
    }
}
