package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;


/**
 * Spark implementation of the {@link SparkBernoulliSampleOperator}.
 */
public class SparkBernoulliSampleOperator<Type>
        extends SampleOperator<Type>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param sampleSize
     */
    public SparkBernoulliSampleOperator(int sampleSize, DataSetType type) {
        super(sampleSize, type);
    }

    /**
     * Creates a new instance.
     *
     * @param sampleSize
     * @param datasetSize
     */
    public SparkBernoulliSampleOperator(int sampleSize, long datasetSize, DataSetType type) {
        super(sampleSize, datasetSize, type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        double sampleFraction;

        final JavaRDD<Type> inputRdd = inputs[0].provideRdd();
        if (datasetSize > 0) //sample size was given as input
            sampleFraction = ((double)sampleSize) / datasetSize;
        else
            sampleFraction = ((double) sampleSize) / inputRdd.cache().count();
        final JavaRDD<Type> outputRdd = inputRdd.sample(false, sampleFraction);

        outputs[0].acceptRdd(outputRdd);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkBernoulliSampleOperator<>(this.sampleSize, this.datasetSize, this.getType());
    }
}