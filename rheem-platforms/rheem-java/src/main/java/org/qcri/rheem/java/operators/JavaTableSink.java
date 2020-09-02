package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.TableSink;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class JavaTableSink extends TableSink implements JavaExecutionOperator {

    private String mode;

    public JavaTableSink(Properties props, String mode, String tableName) {
        this(props, mode, tableName, null);
    }

    public JavaTableSink(Properties props, String mode, String tableName, String... columnNames) {
        super(props, tableName, columnNames);
        this.mode = mode;
    }

    public JavaTableSink(Properties props, String mode, String tableName, String[] columnNames, DataSetType<Record> type) {
        super(props, tableName, columnNames, type);
        this.mode = mode;
    }

    public JavaTableSink(TableSink that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == 1;
        assert outputs.length == 0;
        JavaChannelInstance input = (JavaChannelInstance) inputs[0];

        // The stream is converted to an Iterator so that we can read the first element w/o consuming the entire stream.
        Iterator<Record> recordIterator = input.<Record>provideStream().iterator();
        // We read the first element to derive the Record schema.
        Record schemaRecord = recordIterator.next();

        // We assume that all records have the same length and only check the first record.
        int recordLength = schemaRecord.size();
        if (this.getColumnNames() != null) {
            assert recordLength == this.getColumnNames().length;
        } else {
            String[] columnNames = new String[recordLength];
            for (int i = 0; i < recordLength; i++) {
                columnNames[i] = "c_" + i;
            }
            this.setColumnNames(columnNames);
        }

        // TODO: Check if we need this property.
        this.getProperties().setProperty("streamingBatchInsert", "True");

        Connection conn;
        try {
            Class.forName(this.getProperties().getProperty("driver"));
            conn = DriverManager.getConnection(this.getProperties().getProperty("url"), this.getProperties());
            conn.setAutoCommit(false);
            
            Statement stmt = conn.createStatement();

            // Drop existing table if the mode is 'overwrite'.
            if (this.mode.equals("overwrite")) {
                stmt.execute("DROP TABLE IF EXISTS " + this.getTableName());
            }

            // Create a new table if the specified table name does not exist yet.
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE IF NOT EXISTS ").append(this.getTableName()).append(" (");
            String separator = "";
            for (int i = 0; i < recordLength; i++) {
                sb.append(separator).append(this.getColumnNames()[i]).append(" VARCHAR(255)");
                separator = ", ";
            }
            sb.append(")");
            stmt.execute(sb.toString());

            // Create a prepared statement to insert value from the recordIterator.
            sb = new StringBuilder();
            sb.append("INSERT INTO ").append(this.getTableName()).append(" (");
            separator = "";
            for (int i = 0; i < recordLength; i++) {
                sb.append(separator).append(this.getColumnNames()[i]);
                separator = ", ";
            }
            sb.append(") VALUES (");
            separator = "";
            for (int i = 0; i < recordLength; i++) {
                sb.append(separator).append("?");
                separator = ", ";
            }
            sb.append(")");
            PreparedStatement ps = conn.prepareStatement(sb.toString());

            // The schema Record has to be pushed to the database too.
            for (int i = 0; i < recordLength; i++) {
                ps.setString(i + 1, schemaRecord.getString(i));
            }
            ps.addBatch();

            // Iterate through all remaining records and add them to the prepared statement
            recordIterator.forEachRemaining(
                    r -> {
                        try {
                            for (int i = 0; i < recordLength; i++) {
                                ps.setString(i + 1, r.getString(i));
                            }
                            ps.addBatch();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
            );

            ps.executeBatch();
            conn.commit();
            conn.close();
        } catch (ClassNotFoundException e) {
            System.out.println("Please specify a correct database driver.");
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.tablesink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no outputs.");
    }
}
