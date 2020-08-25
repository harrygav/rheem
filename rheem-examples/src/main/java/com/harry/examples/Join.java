package com.harry.examples;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.hive.Hive;
import org.qcri.rheem.java.Java;

public class Join {

    public static void main(String[] args) {

        Operator src1 = new TextFileSource("file:///home/harry/datasets/users.csv");
        Operator parse1 = new MapOperator<String, Record>(t -> new Record(t.split(",")), String.class, Record.class);
        src1.connectTo(0, parse1, 0);

        Operator src2 = new TextFileSource("file:///home/harry/datasets/images.csv");
        Operator parse2 = new MapOperator<String, Record>(t -> new Record(t.split(",")), String.class, Record.class);

        src2.connectTo(0, parse2, 0);

        Operator join1 = new JoinOperator<Record, Record, String>(t -> t.getString(0), t -> t.getString(0), Record.class, Record.class, String.class);

        parse1.connectTo(0, join1, 0);
        parse2.connectTo(0, join1, 1);
        Operator flatten1 = new MapOperator<Tuple2, Record>(t -> {
            Record r1 = (Record) t.getField0();
            Record r2 = (Record) t.getField1();

            return new Record(r1.getField(0), r2.getField(1));
        }, Tuple2.class, Record.class);
        join1.connectTo(0, flatten1, 0);

        Operator src3 = new TextFileSource("file:///polydb/src/main/resources/sales/region2.csv");
        Operator parse3 = new MapOperator<String, Record>(t -> new Record(t.split(",")), String.class, Record.class);
        src3.connectTo(0, parse3, 0);

        Operator join2 = new JoinOperator<Record, Record, String>(t -> t.getString(0), t -> t.getString(0), Record.class, Record.class, String.class);

        parse3.connectTo(0, join2, 1);
        flatten1.connectTo(0, join2, 0);

        Operator flatten2 = new MapOperator<Tuple2, Record>(t -> {
            Record r1 = (Record) t.getField0();
            Record r2 = (Record) t.getField1();

            return new Record(r1.getField(0), r1.getField(1), r2.getField(1));
        }, Tuple2.class, Record.class);

        join2.connectTo(0, flatten2, 0);


        Operator sink = LocalCallbackSink.createStdoutSink(Record.class);
        flatten2.connectTo(0, sink, 0);
        RheemPlan plan = new RheemPlan(sink);

        // Create context and execute.
        RheemContext rheemContext = new RheemContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Hive.plugin());

        Job job = rheemContext.createJob("Java API Join Example", plan);
        job.execute();
    }
}
