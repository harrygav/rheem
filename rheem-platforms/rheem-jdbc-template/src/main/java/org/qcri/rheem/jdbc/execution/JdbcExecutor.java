package org.qcri.rheem.jdbc.execution;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ExecutionState;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.ExecutorTemplate;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.jdbc.channels.SqlQueryChannel;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;
import org.qcri.rheem.jdbc.operators.*;
import org.qcri.rheem.jdbc.platform.JdbcPlatformTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link Executor} implementation for the {@link JdbcPlatformTemplate}.
 */
public class JdbcExecutor extends ExecutorTemplate {

    private final JdbcPlatformTemplate platform;

    private final Connection connection;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final FunctionCompiler functionCompiler = new FunctionCompiler();

    public JdbcExecutor(JdbcPlatformTemplate platform, Job job) {
        super(job.getCrossPlatformExecutor());
        this.platform = platform;
        this.connection = this.platform.createDatabaseDescriptor(job.getConfiguration()).createJdbcConnection();
    }

    @Override
    public void execute(ExecutionStage stage, OptimizationContext optimizationContext, ExecutionState executionState) {
        // TODO: Load ChannelInstances from executionState? (as of now there is no input into PostgreSQL).
        Set<ExecutionTask> allTasks = stage.getAllTasks();
        Collection<ExecutionTask> startTasks = stage.getStartTasks();
        Collection<ExecutionTask> termTasks = stage.getTerminalTasks();

        // Verify that we can handle this instance.
        startTasks.forEach(task -> {
            assert task.getOperator() instanceof TableSource : "Invalid JDBC stage: Start task has to be a TableSource";
        });
        assert termTasks.size() == 1 : "Invalid JDBC stage: multiple terminal tasks are not currently supported.";

        // Extract the different types of ExecutionOperators from the stage.
        Collection<ExecutionTask> tableSources = new ArrayList<>();
        Collection<ExecutionTask> joinTasks = new ArrayList<>();
        Collection<ExecutionTask> filterTasks = new ArrayList<>();
        Collection<ExecutionTask> projectionTasks = new ArrayList<>();

        SqlQueryChannel.Instance tipChannelInstance = null;
        for (ExecutionTask startTask : startTasks) {
            tableSources.add(startTask);
            tipChannelInstance = this.instantiateOutboundChannel(startTask, optimizationContext);
            ExecutionTask nextTask = this.findJdbcExecutionOperatorTaskInStage(startTask, stage);

            while (nextTask != null) {
                // Evaluate the nextTask.
                if (nextTask.getOperator() instanceof JdbcTableSource) {
                    throw new RheemException(String.format(
                            "A TableSource operator cannot be applied on another TableSource operator: %s",
                            nextTask.toString()));
                } else if (nextTask.getOperator() instanceof JdbcJoinOperator) {
                    if (!joinTasks.contains(nextTask)) {
                        joinTasks.add(nextTask);
                    }
                } else if (nextTask.getOperator() instanceof JdbcFilterOperator) {
                    filterTasks.add(nextTask);
                } else if (nextTask.getOperator() instanceof JdbcProjectionOperator) {
                    projectionTasks.add(nextTask);
                } else {
                    throw new RheemException(String.format(
                            "Unsupported JDBC execution task %s", nextTask.toString()));
                }

                // Move the tipChannelInstance.
                tipChannelInstance = this.instantiateOutboundChannel(nextTask, optimizationContext, tipChannelInstance);

                // Go to the next nextTask.
                nextTask = this.findJdbcExecutionOperatorTaskInStage(nextTask, stage);
            }
        }

        // Create the SQL query.
        Collection<String> tables = tableSources.stream()
                .map(ExecutionTask::getOperator)
                .map(this::getSqlClause)
                .collect(Collectors.toList());
        Collection<String> conditions = filterTasks.stream()
                .map(ExecutionTask::getOperator)
                .map(this::getSqlClause)
                .collect(Collectors.toList());
        Collection<String> joins = joinTasks.stream()
                .map(ExecutionTask::getOperator)
                .map(this::getSqlClause)
                .collect(Collectors.toList());
        conditions.addAll(joins);
        Collection<String> projections = projectionTasks.stream()
                .map(ExecutionTask::getOperator)
                .map(this::getSqlClause)
                .collect(Collectors.toList());
        String query = this.createSqlQuery(tables, conditions, projections);
        tipChannelInstance.setSqlQuery(query);

        // Return the tipChannelInstance.
        executionState.register(tipChannelInstance);
    }

    /**
     * Retrieves the follow-up {@link ExecutionTask} of the given {@code task} unless it is not comprising a
     * {@link JdbcExecutionOperator} and/or not in the given {@link ExecutionStage}.
     *
     * @param task  whose follow-up {@link ExecutionTask} is requested; should have a single follower
     * @param stage in which the follow-up {@link ExecutionTask} should be
     * @return the said follow-up {@link ExecutionTask} or {@code null} if none
     */
    private ExecutionTask findJdbcExecutionOperatorTaskInStage(ExecutionTask task, ExecutionStage stage) {
        assert task.getNumOuputChannels() == 1;
        final Channel outputChannel = task.getOutputChannel(0);
        final ExecutionTask consumer = RheemCollections.getSingle(outputChannel.getConsumers());
        return consumer.getStage() == stage && consumer.getOperator() instanceof JdbcExecutionOperator ?
                consumer :
                null;
    }

    /**
     * Instantiates the outbound {@link SqlQueryChannel} of an {@link ExecutionTask}.
     *
     * @param task                whose outbound {@link SqlQueryChannel} should be instantiated
     * @param optimizationContext provides information about the {@link ExecutionTask}
     * @return the {@link SqlQueryChannel.Instance}
     */
    private SqlQueryChannel.Instance instantiateOutboundChannel(ExecutionTask task,
                                                                OptimizationContext optimizationContext) {
        assert task.getNumOuputChannels() == 1 : String.format("Illegal task: %s.", task);
        assert task.getOutputChannel(0) instanceof SqlQueryChannel : String.format("Illegal task: %s.", task);

        SqlQueryChannel outputChannel = (SqlQueryChannel) task.getOutputChannel(0);
        OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(task.getOperator());
        return outputChannel.createInstance(this, operatorContext, 0);
    }

    /**
     * Instantiates the outbound {@link SqlQueryChannel} of an {@link ExecutionTask}.
     *
     * @param task                       whose outbound {@link SqlQueryChannel} should be instantiated
     * @param optimizationContext        provides information about the {@link ExecutionTask}
     * @param predecessorChannelInstance preceding {@link SqlQueryChannel.Instance} to keep track of lineage
     * @return the {@link SqlQueryChannel.Instance}
     */
    private SqlQueryChannel.Instance instantiateOutboundChannel(ExecutionTask task,
                                                                OptimizationContext optimizationContext,
                                                                SqlQueryChannel.Instance predecessorChannelInstance) {
        final SqlQueryChannel.Instance newInstance = this.instantiateOutboundChannel(task, optimizationContext);
        newInstance.getLineage().addPredecessor(predecessorChannelInstance.getLineage());
        return newInstance;
    }

    /**
     * Creates a SQL query.
     *
     * @param tables      the tables to be queried
     * @param conditions  conditions for the {@code WHERE} clause
     * @param projections projections for the {@code SELECT} clause
     * @return the SQL query
     */
    protected String createSqlQuery(Collection<String> tables,
                                    Collection<String> conditions,
                                    Collection<String> projections) {
        StringBuilder sb = new StringBuilder(1000);
        if (!projections.isEmpty()) {
            sb.append("SELECT ");
            String separator = "";
            for (String projection : projections) {
                sb.append(separator).append(projection);
                separator = ", ";
            }
        } else {
            sb.append("SELECT *");
        }
        if (!tables.isEmpty()) {
            sb.append(" FROM ");
            String separator = "";
            for (String table : tables) {
                sb.append(separator).append(table);
                separator = ", ";
            }
        } else {
            throw new RheemException("No table sources were given for the SQL query.");
        }
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            String separator = "";
            for (String condition : conditions) {
                sb.append(separator).append(condition);
                separator = " AND ";
            }
        }

        // Print SQL output for testing
        //System.out.println("SQL Query:\n" + sb.toString());

        // The semicolon needs to be omitted on some platforms
        if (!(getPlatform().getName().toLowerCase().equals("phoenix") || getPlatform().getName().toLowerCase().equals("hive")))
            sb.append(';');

        return sb.toString();
    }

    /**
     * Creates a SQL clause that corresponds to the given {@link Operator}.
     *
     * @param operator for that the SQL clause should be generated
     * @return the SQL clause
     */
    private String getSqlClause(Operator operator) {
        return ((JdbcExecutionOperator) operator).createSqlClause(this.connection, this.functionCompiler);
    }

    @Override
    public void dispose() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            this.logger.error("Could not close JDBC connection to PostgreSQL correctly.", e);
        }
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }


    private void saveResult(FileChannel.Instance outputFileChannelInstance, ResultSet rs) throws IOException, SQLException {
        // Output results.
        final FileSystem outFs = FileSystems.getFileSystem(outputFileChannelInstance.getSinglePath()).get();
        try (final OutputStreamWriter writer = new OutputStreamWriter(outFs.create(outputFileChannelInstance.getSinglePath()))) {
            while (rs.next()) {
                //System.out.println(rs.getInt(1) + " " + rs.getString(2));
                ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    writer.write(rs.getString(i));
                    if (i < rsmd.getColumnCount()) {
                        writer.write('\t');
                    }
                }
                if (!rs.isLast()) {
                    writer.write('\n');
                }
            }
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
