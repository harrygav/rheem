package com.harry.examples;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.postgres.Postgres;
import org.qcri.rheem.postgres.operators.PostgresTableSource;
import org.qcri.rheem.spark.Spark;

public class PostgresSparkExample {

    public static void main(String[] args) {


        Operator source = new PostgresTableSource("aka_name");

        Operator project = MapOperator.createProjection(Record.class, Record.class, new String[]{"person_id", "name"});

        source.connectTo(0, project, 0);

        PredicateDescriptor pred = new PredicateDescriptor(t -> true, Record.class);
        Operator select = new FilterOperator<Record>(pred);
        select.addTargetPlatform(Spark.platform());

        project.connectTo(0, select, 0);

        Operator sink = new TextFileSink("file:///tmp/rheemtest.csv", Record.class);

        select.connectTo(0, sink, 0);

        RheemPlan plan = new RheemPlan(sink);

        //initialize rheem context
        RheemContext context = new RheemContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spark.basicPlugin());

        //create & execute rheem job
        Job job = context.createJob("job", plan);
        job.execute();
    }
}
