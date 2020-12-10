package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.qcri.rheem.basic.operators.SqlStatementSource;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.jdbc.channels.SqlQueryChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see SparkObjectFileSink
 */
public class SparkSqlStatementSource extends SqlStatementSource implements SparkExecutionOperator {


    public SparkSqlStatementSource(String sqlStamenent, Properties props) {
        super(sqlStamenent, props);
    }

    public SparkSqlStatementSource(SqlStatementSource that) {
        super(that);
    }

    public SparkSqlStatementSource() {
        super(null);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        RddChannel.Instance output = (RddChannel.Instance) outputs[0];
        SQLContext sqlContext = new SQLContext(sparkExecutor.sc);

        Dataset<Row> jdbcDS = sqlContext.read()
                .format("jdbc")
                .option("url", this.getProperties().getProperty("url"))
                .option("dbtable", "(" + this.getSqlStatement() + ") q_alias")
                .option("user", this.getProperties().getProperty("user"))
                .option("password", this.getProperties().getProperty("password"))
                .load();


        final JavaRDD rdd = jdbcDS.rdd().toJavaRDD();
        this.name(rdd);


        output.accept(rdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }


    @Override
    protected ExecutionOperator createCopy() {
        return new SparkSqlStatementSource(this.getSqlStatement(), this.getProperties());
    }


    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(SqlQueryChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}
