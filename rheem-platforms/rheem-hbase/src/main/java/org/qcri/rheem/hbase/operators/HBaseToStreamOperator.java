package org.qcri.rheem.hbase.operators;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.hbase.channels.HBaseQueryChannel;
import org.qcri.rheem.hbase.platform.HBasePlatform;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Converts {@link StreamChannel} into a {@link CollectionChannel}
 */
public class HBaseToStreamOperator extends UnaryToUnaryOperator<Record, Record> implements JavaExecutionOperator {

    private final HBasePlatform hBasePlatform;


    public HBaseToStreamOperator(HBasePlatform hBasePlatform) {
        this(hBasePlatform, DataSetType.createDefault(Record.class));
    }

    public HBaseToStreamOperator(HBasePlatform hBasePlatform, DataSetType<Record> dataSetType) {
        super(dataSetType, dataSetType, false);
        this.hBasePlatform = hBasePlatform;
    }

    protected HBaseToStreamOperator(HBaseToStreamOperator that) {
        super(that);
        this.hBasePlatform = that.hBasePlatform;
    }


    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        // Cast the inputs and outputs
        final HBaseQueryChannel.Instance input = (HBaseQueryChannel.Instance) inputs[0];
        final StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];


        //TODO: finish resultset
        Iterator<Record> resultSetIterator = new HBaseToStreamOperator.ResultSetIterator(input.getHbaseScan(), input.getTable(), input.getProjectedFields());
        Spliterator<Record> resultSetSpliterator = Spliterators.spliteratorUnknownSize(resultSetIterator, 0);
        Stream<Record> resultSetStream = StreamSupport.stream(resultSetSpliterator, false);


        output.accept(resultSetStream);

        ExecutionLineageNode queryLineageNode = new ExecutionLineageNode(operatorContext);
        queryLineageNode.addPredecessor(input.getLineage());
        ExecutionLineageNode outputLineageNode = new ExecutionLineageNode(operatorContext);
        output.getLineage().addPredecessor(outputLineageNode);

        return queryLineageNode.collectAndMark();
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(this.hBasePlatform.getHbaseQueryChannelDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    /**
     * Exposes a {@link ResultScanner} as an {@link Iterator}.
     */
    private static class ResultSetIterator implements Iterator<Record>, AutoCloseable {

        private Iterator<Result> resultIterator;
        private ArrayList<String> projectedFields;
        private Record next;

        ResultSetIterator(Scan hbaseScan, Table hbaseTable, ArrayList<String> projectedFields) {

            try {

                this.resultIterator = hbaseTable.getScanner(hbaseScan).iterator();
                this.resultIterator.next();
                this.projectedFields = projectedFields;

            } catch (IOException e) {
                e.printStackTrace();
            }

            this.moveToNext();
        }

        /**
         * Moves this instance to the next {@link Record}.
         */
        private void moveToNext() {
            if (this.resultIterator == null || !this.resultIterator.hasNext()) {
                this.next = null;
                this.close();

            } else {

                byte[] family = "cf".getBytes();
                Result curRes = this.resultIterator.next();
                Object[] values;
                if (this.projectedFields == null) {

                    NavigableMap familyMap = curRes.getFamilyMap(Bytes.toBytes("cf"));
                    values = new Object[familyMap.size()];
                    //TODO: check order of record fields
                    int i = familyMap.size();
                    for (Object col : familyMap.values()) {
                        values[i - 1] = Bytes.toString(((byte[]) col));
                        i--;
                    }


                } else {
                    values = new Object[this.projectedFields.size()];

                    for (int i = 0; i < this.projectedFields.size(); i++) {

                        values[i] = (Bytes.toString(curRes.getValue(family, this.projectedFields.get(i).getBytes())));
                    }

                }
                this.next = new Record(values);

            }
        }

        @Override
        public boolean hasNext() {
            return this.next != null;
        }

        @Override
        public Record next() {
            Record curNext = this.next;
            this.moveToNext();
            return curNext;
        }

        @Override
        public void close() {
            this.resultIterator = null;
        }
    }
}
