package com.lennart.examples;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.hive.Hive;
import org.qcri.rheem.hive.operators.HiveTableSource;
import org.qcri.rheem.java.Java;

public class LowLevelJoin {
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

        // Join.
        Operator join = new JoinOperator<Record, Record, Integer>(
                (FunctionDescriptor.SerializableFunction<Record, Integer>) r -> r.getInt(0),
                (FunctionDescriptor.SerializableFunction<Record, Integer>) r -> r.getInt(0),
                Record.class, Record.class, Integer.class)
                .withSqlImplementation("u_data.userid = u_user.userid");

        selection.connectTo(0, join, 0);
        selection2.connectTo(0, join, 1);

        // Sink.
        // Operator sink = LocalCallbackSink.createStdoutSink(); Which type do I have to use here?
        Operator sink = new TextFileSink<>("file:///home/lbhm/hive/jointest.csv", Tuple2.class);
        join.connectTo(0, sink, 0);

        // Create RheemPlan
        RheemPlan plan = new RheemPlan(sink);

        // Create context and execute.
        RheemContext rheemContext = new RheemContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Hive.plugin());

        Job job = rheemContext.createJob("Low Level Join Example", plan);
        job.execute();
    }
}
