package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.SqlStatementSource;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.SqlStatementChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.io.File;
import java.util.*;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see SparkObjectFileSink
 */
public class SparkHiveTableSource extends SqlStatementSource implements SparkExecutionOperator {


    public SparkHiveTableSource() {
        this(null, null);
    }

    public SparkHiveTableSource(String sqlStamenent, Properties props) {
        super(sqlStamenent, props);
    }

    public SparkHiveTableSource(SqlStatementSource that) {
        super(that);
    }


    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        RddChannel.Instance output = (RddChannel.Instance) outputs[0];
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession ss = SparkSession
                .builder()
                .config(sparkExecutor.sc.getConf())
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("hbase.zookeeper.quorum", "zoo")
                .config("hive.metastore.uris", "thrift://hive-metastore:9083")
                .config("javax.jdo.option.ConnectionUserName", "hive")
                .config("hive.server2.enable.doAs", "false")
                .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres-metastore/metastore")
                .config("datanucleus.autoCreateSchema", "false")
                .config("hive.metastore.event.db.notification.api.auth", "false")
                .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
                .config("javax.jdo.option.ConnectionPassword", "hivePW123")
                .config("spark.driver.extraClassPath", "hdfs:///jars/hive-hbase-handler-2.3.8.jar,hdfs:///jars/hbase-client-2.2.6.jar,hdfs:///jars/hbase-server-2.2.6.jar,hdfs:///jars/hbase-protocol-shaded-2.2.6.jar,hdfs:///jars/hbase-common-2.2.6.jar,hdfs:///jars/hbase-mapreduce-2.2.6.jar,hdfs:///jars/hbase-shaded-miscellaneous-2.2.1.jar,hdfs:///jars/hbase-shaded-protobuf-2.2.1.jar,hdfs:///jars/hbase-shaded-netty-2.2.1.jar,hdfs:///jars/htrace-core4-4.2.0-incubating.jar,hdfs:///jars/hbase-protocol-2.2.6.jar")
                .config("hbase.cluster.distributed", "false")
                .config("hbase.tmp.dir", "./tmp")
                .config("hbase.unsafe.stream.capability.enforce", "false")
                .config("hbase.rootdir", "hdfs:///hbase")
                .config("hbase.master.port", "16000")
                .config("hbase.cluster.distributed", "true")
                .config("hbase.manages.zk", "false")
                .config("hbase.regionserver.info.port", "16030")
                .config("zookeeper.session.timeout", "120000")
                .config("hbase.master.info.port", "16010")
                .config("hbase.zookeeper.quorum", "zoo")
                .config("hbase.master", "hbase-master:16000")
                .config("hbase.zookeeper.property.tickTime", "6000")
                .config("hbase.regionserver.port", "16020")
                .config("hbase.master.hostname", "hbase-master")

                .config("spark.files", "file:///hive-site.xml")
                .enableHiveSupport()
                .getOrCreate();

        final SqlStatementChannel.Instance input = (SqlStatementChannel.Instance) inputs[0];
        Properties props = input.getProps();

        Dataset<Row> jdbcDS = ss.sql(input.getSqlStatement().replaceAll(";$", ""));

        System.out.println("(" + input.getSqlStatement().replaceAll(";$", "") + ") q_alias");
//        jdbcDS.explain();
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

        ExecutionLineageNode queryLineageNode = new ExecutionLineageNode(operatorContext);
        queryLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "rheem.spark.hivetablesource.load.query", sparkExecutor.getConfiguration()));

        queryLineageNode.addPredecessor(input.getLineage());
        ExecutionLineageNode outputLineageNode = new ExecutionLineageNode(operatorContext);
        outputLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "rheem.spark.hivetablesource.load.query", sparkExecutor.getConfiguration()
        ));
        output.getLineage().addPredecessor(outputLineageNode);
        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("rheem.spark.hivetablesource.load.main");
    }


    @Override
    protected ExecutionOperator createCopy() {
        return new SparkHiveTableSource(this.getSqlStatement(), this.getProperties());
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


}
