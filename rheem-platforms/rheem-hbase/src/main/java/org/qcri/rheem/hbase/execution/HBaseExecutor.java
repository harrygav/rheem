package org.qcri.rheem.hbase.execution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ExecutionState;
import org.qcri.rheem.core.platform.ExecutorTemplate;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.hbase.channels.HBaseQueryChannel;
import org.qcri.rheem.hbase.operators.HBaseExecutionOperator;
import org.qcri.rheem.hbase.operators.HBaseFilterOperator;
import org.qcri.rheem.hbase.operators.HBaseProjectionOperator;
import org.qcri.rheem.hbase.platform.HBasePlatform;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

public class HBaseExecutor extends ExecutorTemplate {

    private final HBasePlatform platform;

    public HBaseExecutor(HBasePlatform platform, Job job) {
        super(job.getCrossPlatformExecutor());
        this.platform = platform;
    }

    @Override
    public void execute(ExecutionStage stage, OptimizationContext optimizationContext, ExecutionState executionState) {
        Collection<?> startTasks = stage.getStartTasks();
        Collection<?> termTasks = stage.getTerminalTasks();

        // Verify that we can handle this instance.
        assert startTasks.size() == 1 : "Invalid HBase stage: multiple sources are not currently supported";
        ExecutionTask startTask = (ExecutionTask) startTasks.toArray()[0];
        assert termTasks.size() == 1 : "Invalid HBase stage: multiple terminal tasks are not currently supported.";
        ExecutionTask termTask = (ExecutionTask) termTasks.toArray()[0];
        assert startTask.getOperator() instanceof TableSource : "Invalid HBase stage: Start task has to be a TableSource";

        // Extract the different types of ExecutionOperators from the stage.
        TableSource tableOp = (TableSource) startTask.getOperator();
        HBaseQueryChannel.Instance tipChannelInstance = this.instantiateOutboundChannel(startTask, optimizationContext);
        Collection<ExecutionTask> filterTasks = new ArrayList<>();
        ExecutionTask projectionTask = null;
        Set<ExecutionTask> allTasks = stage.getAllTasks();
        assert allTasks.size() <= 3;
        ExecutionTask nextTask = this.findHBaseExecutionOperatorTaskInStage(startTask, stage);
        while (nextTask != null) {
            // Evaluate the nextTask.
            if (nextTask.getOperator() instanceof HBaseFilterOperator) {
                filterTasks.add(nextTask);
            } else if (nextTask.getOperator() instanceof HBaseProjectionOperator) {
                assert projectionTask == null; //Allow one projection operator per stage for now.
                projectionTask = nextTask;

            } else {
                throw new RheemException(String.format("Unsupported HBase execution task %s", nextTask.toString()));
            }

            // Move the tipChannelInstance.
            tipChannelInstance = this.instantiateOutboundChannel(nextTask, optimizationContext, tipChannelInstance);

            // Go to the next nextTask.
            nextTask = this.findHBaseExecutionOperatorTaskInStage(nextTask, stage);
        }


        // Return the tipChannelInstance.
        executionState.register(tipChannelInstance);

        // Create the HBase query.
        Configuration config = HBaseConfiguration.create();

        try {
            HBaseAdmin.available(config);
            //table source
            String tableName = tableOp.getTableName();
            Connection connection = ConnectionFactory.createConnection(config);
            TableName table1 = TableName.valueOf(tableName);
            Table table = connection.getTable(table1);
            tipChannelInstance.setTable(table);
            Scan scan = new Scan();


            //project operator
            ArrayList<String> cols = new ArrayList<>();
            byte[] family = "cf".getBytes();
            HBaseProjectionOperator projectionOperator = (HBaseProjectionOperator) projectionTask.getOperator();
            ProjectionDescriptor projectionDescriptor = (ProjectionDescriptor) projectionOperator.getFunctionDescriptor();
            for (Object field : projectionDescriptor.getFieldNames()) {
                scan.addColumn(family, ((String) field).getBytes());
                cols.add((String) field);
            }
            tipChannelInstance.setProjectedFields(cols);

            ArrayList<Filter> filters = new ArrayList<>();
            //select operator
            for (ExecutionTask filterTask : filterTasks) {
                HBaseFilterOperator filterOperator = (HBaseFilterOperator) filterTask.getOperator();
                PredicateDescriptor predicateDescriptor = filterOperator.getPredicateDescriptor();
                String sqlPredicate = predicateDescriptor.getSqlImplementation();
                //TODO: extend
                byte[] col = sqlPredicate.split("=")[0].getBytes();
                byte[] val = sqlPredicate.split("=")[1].getBytes();

                Filter filter = new SingleColumnValueFilter(family, col, CompareOperator.EQUAL, val);
                filters.add(filter);
            }


            scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, filters));

            tipChannelInstance.setHbaseScan(scan);
            executionState.register(tipChannelInstance);
            //execution operator
            /*ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println("Row: ");

                for (Object field : projectionDescriptor.getFieldNames()) {
                    System.out.println("Col: " + Bytes.toString(result.getValue(family, ((String) field).getBytes())));
                }

            }*/
        } catch (IOException e) {

            System.out.println(e.getMessage());
        }

    }

    /**
     * Retrieves the follow-up {@link ExecutionTask} of the given {@code task} unless it is not comprising a
     * {@link HBaseExecutionOperator} and/or not in the given {@link ExecutionStage}.
     *
     * @param task  whose follow-up {@link ExecutionTask} is requested; should have a single follower
     * @param stage in which the follow-up {@link ExecutionTask} should be
     * @return the said follow-up {@link ExecutionTask} or {@code null} if none
     */
    private ExecutionTask findHBaseExecutionOperatorTaskInStage(ExecutionTask task, ExecutionStage stage) {
        assert task.getNumOuputChannels() == 1;
        final Channel outputChannel = task.getOutputChannel(0);
        final ExecutionTask consumer = RheemCollections.getSingle(outputChannel.getConsumers());
        return consumer.getStage() == stage && consumer.getOperator() instanceof HBaseExecutionOperator ?
                consumer :
                null;
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }

    private HBaseQueryChannel.Instance instantiateOutboundChannel(ExecutionTask task,
                                                                  OptimizationContext optimizationContext) {
        assert task.getNumOuputChannels() == 1 : String.format("Illegal task: %s.", task);
        assert task.getOutputChannel(0) instanceof HBaseQueryChannel : String.format("Illegal task: %s.", task);

        HBaseQueryChannel outputChannel = (HBaseQueryChannel) task.getOutputChannel(0);
        OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(task.getOperator());
        return outputChannel.createInstance(this, operatorContext, 0);
    }

    private HBaseQueryChannel.Instance instantiateOutboundChannel(ExecutionTask task,
                                                                  OptimizationContext optimizationContext,
                                                                  HBaseQueryChannel.Instance predecessorChannelInstance) {
        final HBaseQueryChannel.Instance newInstance = this.instantiateOutboundChannel(task, optimizationContext);
        newInstance.getLineage().addPredecessor(predecessorChannelInstance.getLineage());
        return newInstance;
    }
}
