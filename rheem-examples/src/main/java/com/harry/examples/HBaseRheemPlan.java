package com.harry.examples;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.hbase.HBase;
import org.qcri.rheem.hbase.operators.HBaseTableSource;
import org.qcri.rheem.java.Java;

public class HBaseRheemPlan {
    public static void main(String[] args) {

        //table source
        Operator source = new HBaseTableSource("nation");

        //projection
        Operator project = MapOperator.createProjection(Record.class, Record.class, new String[]{"n_regionkey", "n_name", "n_comment"});

        //connect table source to project
        source.connectTo(0, project, 0);

        //selection
        PredicateDescriptor pred = new PredicateDescriptor(t -> true, Record.class).withSqlImplementation("n_regionkey=0");
        Operator select = new FilterOperator<Record>(pred);

        //connect selection to projection
        project.connectTo(0, select, 0);

        //sink
        Operator sink = new TextFileSink("file:///home/harry/test.csv", Record.class);

        //connect select to sink
        select.connectTo(0, sink, 0);

        //create rheem plan
        RheemPlan plan = new RheemPlan(sink);

        //initialize rheem context
        RheemContext context = new RheemContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(HBase.plugin());

        //create & execute rheem job
        Job job = context.createJob("job", plan);
        job.execute();

    }
}
