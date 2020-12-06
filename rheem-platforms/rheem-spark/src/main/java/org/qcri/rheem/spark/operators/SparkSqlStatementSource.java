package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.SqlStatementSource;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.SqlStatementChannel;
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


    public SparkSqlStatementSource() {
        this(null, null);
    }

    public SparkSqlStatementSource(String sqlStamenent, Properties props) {
        super(sqlStamenent, props);
    }

    public SparkSqlStatementSource(SqlStatementSource that) {
        super(that);
    }


    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        RddChannel.Instance output = (RddChannel.Instance) outputs[0];
        SQLContext sqlContext = new SQLContext(sparkExecutor.sc);

        final SqlStatementChannel.Instance input = (SqlStatementChannel.Instance) inputs[0];
        Properties props = input.getProps();

        JdbcDialects.registerDialect(new HiveDialect());

        Dataset<Row> jdbcDS = sqlContext.read()
                .format("jdbc")
                .option("url", props.getProperty("url"))
                .option("dbtable", "(" + input.getSqlStatement().replaceAll(";$", "") + ") q_alias")
                .option("user", props.getProperty("user"))
                .option("password", props.getProperty("password"))
                .option("driver", props.getProperty("driver"))
                .load();


        final JavaRDD<Row> rdd1 = jdbcDS.rdd().toJavaRDD();

        final JavaRDD<Record> rdd = rdd1.map(r -> {

            String[] vals = new String[r.length()];
            for (int i = 0; i < r.length(); i++) {

                vals[i] = r.getString(i);

            }
            return new Record(vals);
        });
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
        return Collections.singletonList(SqlStatementChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    public class HiveDialect extends JdbcDialect {


        @Override

        public boolean canHandle(String url) {

            return url.startsWith("jdbc:hive2");

        }

        @Override

        public String quoteIdentifier(String colName) {

            return "" + colName + "";

        }


    }

}
