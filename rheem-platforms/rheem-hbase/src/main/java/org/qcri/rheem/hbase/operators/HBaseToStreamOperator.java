package org.qcri.rheem.hbase.operators;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.hbase.platform.HBasePlatform;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.qcri.rheem.hbase.channels.HBaseQueryChannel;
import org.qcri.rheem.jdbc.operators.SqlToStreamOperator;

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
        /*Iterator<Record> resultSetIterator = new ResultSetIterator(connection, input.getSqlQuery());
        Spliterator<Record> resultSetSpliterator = Spliterators.spliteratorUnknownSize(resultSetIterator, 0);
        Stream<Record> resultSetStream = StreamSupport.stream(resultSetSpliterator, false);

        Stream<Record> resultSetStream =
        //execution operator
        ResultScanner scanner = null;
        try {
            byte[] family = "cf".getBytes();
            scanner = input.getTable().getScanner(input.getHbaseScan());
            for (Result result : scanner) {
                System.out.println("Row: ");

                ArrayList<String> recordVals = new ArrayList<>();
                for (String field : input.getProjectedFields()) {
                    recordVals.add(Bytes.toString(result.getValue(family, field.getBytes())));

                }
                Record rec = new Record(recordVals.toArray(new String[recordVals.size()]));

            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        output.accept(resultSetStream);*/

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


}
