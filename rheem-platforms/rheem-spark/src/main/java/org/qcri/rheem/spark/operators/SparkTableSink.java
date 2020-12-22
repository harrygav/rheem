package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.TableSink;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class SparkTableSink extends TableSink implements SparkExecutionOperator {

    private SaveMode mode;

    public SparkTableSink(Properties props, String mode, String tableName, String... columnNames) {
        super(props, mode, tableName, columnNames);
        this.setMode(mode);
    }

    public SparkTableSink(Properties props, String mode, String tableName, String[] columnNames, DataSetType<Record> type) {
        super(props, mode, tableName, columnNames, type);
        this.setMode(mode);
    }

    public SparkTableSink(TableSink that) {
        super(that);
        this.setMode(that.getMode());
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == 1;
        assert outputs.length == 0;

        JavaRDD<Record> recordRDD = ((RddChannel.Instance) inputs[0]).provideRdd();

        //nothing to write if rdd empty
        recordRDD.cache();

        //boolean isEmpty = recordRDD.isEmpty();
        boolean isEmpty = false;
        if (!isEmpty) {
            int recordLength = recordRDD.first().size();

            JavaRDD<Row> rowRDD = recordRDD.map(record -> {
                Object[] values = record.getValues();
                return RowFactory.create(values);
            });

            StructField[] fields = new StructField[recordLength];
            for (int i = 0; i < recordLength; i++) {
                // TODO: Implement a proper logic to assign the correct data types.
                fields[i] = new StructField(this.getColumnNames()[i], DataTypes.StringType, true, Metadata.empty());
            }
            StructType schema = new StructType(fields);

            SQLContext sqlcontext = new SQLContext(sparkExecutor.sc.sc());
            Dataset<Row> dataSet = sqlcontext.createDataFrame(rowRDD, schema);
            this.getProperties().setProperty("batchSize", "250000");
            dataSet.write().mode(this.mode).jdbc(this.getProperties().getProperty("url"), this.getTableName(), this.getProperties());
        } else {
            System.out.println("RDD is empty, nothing to write!");
        }
        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    public void setMode(String mode) {
        if (mode == null) {
            throw new RheemException("Unspecified write mode for SparkTableSink.");
        } else if (mode.equals("append")) {
            this.mode = SaveMode.Append;
        } else if (mode.equals("overwrite")) {
            this.mode = SaveMode.Overwrite;
        } else if (mode.equals("errorIfExists")) {
            this.mode = SaveMode.ErrorIfExists;
        } else if (mode.equals("ignore")) {
            this.mode = SaveMode.Ignore;
        } else {
            throw new RheemException(String.format("Specified write mode for SparkTableSink does not exist: %s", mode));
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no outputs.");
    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.tablesink.load";
    }
}
