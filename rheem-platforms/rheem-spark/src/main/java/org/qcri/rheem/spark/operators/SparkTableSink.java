package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
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
        super(props, tableName, columnNames);
        this.setMode(mode);
    }

    public SparkTableSink(Properties props, String mode, String tableName, String[] columnNames, DataSetType<Record> type) {
        super(props, tableName, columnNames, type);
        this.setMode(mode);
    }

    public SparkTableSink(TableSink that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        System.out.println("STS Evaluate");
        assert inputs.length == 1;
        assert outputs.length == 0;
        JavaRDD<Record> inputRdd = ((RddChannel.Instance) inputs[0]).provideRdd();

        Record schemaRecord = inputRdd.first();
        int recordLength = schemaRecord.size();

        JavaRDD<Row> rowRdd = inputRdd.map(record -> {
            Object[] values = record.getValues();
            Row r = RowFactory.create(values);
            System.out.println("test");
            System.out.println(r);
            return r;
        });

        System.out.println(rowRdd.collect().toString());

        StructField[] fields = new StructField[recordLength];

        for (int i = 0; i < recordLength; i++) {
            fields[i] = new StructField(this.getColumnNames()[i], DataTypes.StringType, true, null);
        }
        StructType schema = new StructType(fields);
        System.out.println("Schema");
        System.out.println(schema.toString());

        SQLContext sqlcontext = new SQLContext(sparkExecutor.sc.sc());

        // TODO: This does not work correctly yet.
        Dataset<Row> dataSet = sqlcontext.createDataFrame(rowRdd, schema);

        System.out.println("DF Content");
        System.out.println(dataSet.collect());
        System.out.println("DF Schema");
        dataSet.printSchema();
        dataSet.write().mode(this.mode).jdbc(this.getProperties().getProperty("url"), this.getTableName(), this.getProperties());

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    public void setMode(String mode) {
        if (mode == null) {
            throw new RheemException("Unspecified write mode for SparkTableSink.");
        } else if (mode.equals("append")) {
            this.mode = SaveMode.Append;
        } else if (mode.equals("overwrite")) {
            this.mode = SaveMode.Overwrite;
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
