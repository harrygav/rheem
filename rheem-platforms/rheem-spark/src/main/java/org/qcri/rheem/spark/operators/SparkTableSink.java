package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
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
        assert inputs.length == 1;
        assert outputs.length == 0;
        JavaRDD<Record> inputRdd = ((RddChannel.Instance) inputs[0]).provideRdd();

        SQLContext sqlcontext=new SQLContext(sparkExecutor.sc.sc());
        Dataset<Row> dataFrame = sqlcontext.createDataFrame(inputRdd, Record.class);
        dataFrame.write().mode(this.mode).jdbc(this.getProperties().getProperty("url"), this.getTableName(), this.getProperties());

//        SQLContext sqlcontext=new SQLContext(context);
//        DataFrame outDataFrame=sqlcontext.createDataFrame(finalOutPutRDD, WebHttpOutPutVO.class);
//        Properties prop = new java.util.Properties();
//        prop.setProperty("database", "Web_Session");
//        prop.setProperty("user", "user");
//        prop.setProperty("password", "pwd@123");
//        prop.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
//        outDataFrame.write().mode(org.apache.spark.sql.SaveMode.Append).jdbc("jdbc:sqlserver://<Host>:1433", "test_table", prop);


//        final Function<T, String> formattingFunction =
//                sparkExecutor.getCompiler().compile(this.formattingDescriptor, this, operatorContext, inputs);
//        inputRdd.map(formattingFunction).saveAsTextFile(this.textFileUrl);

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
