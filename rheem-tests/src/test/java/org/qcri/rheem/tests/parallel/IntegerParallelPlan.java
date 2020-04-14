package org.qcri.rheem.tests.parallel;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.tests.util.PlatformsTest;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link IntegerParallelPlan} is an extension of {@link ParallelPlan} of type {@link Integer}
 *  and the allow the incorporation of the Key to use the in the join
 *
 *  This will generate a plan with the shape bellow.
 *
 * source(platform1)[1] -> map(platform1)[2] -> filter(platform2)[3] \
 *                                                                   join(platform1)[7] -> sink(platform1)[8]
 * source(platform2)[4] -> map(platform2)[5] -> filter(platform1)[6] /
 *
 * the plan described warranties the generation of two independent stages, one composed by the
 * operator [1] and [2] and the other stage by the operators [4] and [5]. The operators [3] and [6]
 * are there to helping the force of the stage that will be executed in parallel.
 *
 * In the case of the operators [2] and [5] are the ones that allow the
 * validation of the execution in parallel because the label the data to know
 * when the tuples it was preceded.
 *
 * @param <Type2> this type will be use after the operators [2] and [5]
 * @param <Key> this is the key that will be using during the join[7]
 */
public abstract class IntegerParallelPlan<Type2, Key> extends ParallelPlan<Integer, Type2> {

    /**
     * key that will be using during the join[7]
     */
    protected final Class<Key> keyClass;

    /**
     * this is the value of the plan in after the execution
     */
    protected Object output;

    /**
     * Constructor of the {@link IntegerParallelPlan}
     *
     * @param type2Class type will be use after the operators [2] and [5]
     * @param keyClass is the class of the key that will be using during the join[7]
     */
    public IntegerParallelPlan(Class<Type2> type2Class, Class<Key> keyClass) {
        super(Integer.class, type2Class, 0, i -> i + 1);
        this.keyClass = keyClass;
    }

    /**
     * return the platform that will be use in the left size of the join[7]
     *
     * @return {@link PlatformsTest} that will be use in the left size
     */
    public PlatformsTest getLeftPlatform(){
        return PlatformsTest.Java;
    }

    /**
     * return the platform that will be use in the right side of the join[7]
     *
     * @return {@link PlatformsTest} that will be use in the right side
     */
    public PlatformsTest getRightPlatform(){
        return PlatformsTest.Spark;
    }


    /**
     * final value of plan in after the execution, this is part
     * of the sink operator
     *
     * @return {@link Object} that represent the output of the plan
     */
    public Object getOutput() {
        return output;
    }

    /**
     * Set the output of the plan, to be recovered when the execution of
     * the plan end
     *
     * @param output is the variable where will be saved the output of the plan
     */
    private void setOutput(Object output) {
        this.output = output;
    }

    /**
     * is the implementation of the {@link ParallelPlan#createPlan()}
     * but in this case the {@link RheemPlan} will have the shape as below:
     *
     * source(platform1)[1] -> map(platform1)[2] -> filter(platform2)[3] \
     *                                                                   join(platform1)[7] -> sink(platform1)[8]
     * source(platform2)[4] -> map(platform2)[5] -> filter(platform1)[6] /
     *
     * the plan described warranties the generation of two independent stages, one composed by the
     * operator [1] and [2] and the other stage by the operators [4] and [5]. The operators [3] and [6]
     * are there to helping the force of the stage that will be executed in parallel.
     *
     * In the case of the operators [2] and [5] are the ones that allow the
     * validation of the execution in parallel because the label the data to know
     * when the tuples it was preceded.
     *
     * @return a {@link RheemPlan} with the shape described.
     */
    @Override
    public RheemPlan createPlan() {

        //Java Size
        CollectionSource<Integer> source_left = this.generateSource(
            this.getSizeSource(),
            this.getLeftPlatform()
        );
        MapOperator<Integer, Type2> map_left = this.generateMapType1_2(
            this.getMapUDF(this.getLeftPlatform().getName()),
            this.getLeftPlatform()
        );
        //in this case, the platform are switched to be sure that will exist
        //an stage can be executed in parralel
        FilterOperator<Type2> filter_left = this.generateFilterType2(
            this.getFilterUDFType2(),
            this.getRightPlatform()
        );

        //Spark Size
        CollectionSource<Integer> source_right = this.generateSource(
            this.getSizeSource(),
            this.getRightPlatform()
        );
        MapOperator<Integer, Type2> map_right = this.generateMapType1_2(
            this.getMapUDF(this.getRightPlatform().getName()),
            this.getRightPlatform()
        );
        //in this case, the platform are switched to be sure that will exist
        //an stage can be executed in parralel
        FilterOperator<Type2> filter_right = this.generateFilterType2(
            this.getFilterUDFType2(),
            this.getLeftPlatform()
        );

        JoinOperator<Type2, Type2, Key> join = this.generateJoinType2(
            this.getKeyExtractorType2(),
            this.getKeyExtractorType2(),
            this.keyClass,
            this.getRightPlatform()
        );

        List<Tuple2<Type2, Type2>> collector = new ArrayList<>();
        this.setOutput(collector);

        LocalCallbackSink<Tuple2<Type2, Type2>> sink = this.generateSink(collector, DataSetType.createDefaultUnchecked(Tuple2.class), this.getLeftPlatform());

        source_left.connectTo(0, map_left, 0);
        map_left.connectTo(0, filter_left, 0);
        filter_left.connectTo(0, join, 0);

        source_right.connectTo(0, map_right, 0);
        map_right.connectTo(0, filter_right, 0);
        filter_right.connectTo(0, join, 1);

        join.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    /**
     * get the size of the elements that will be generate as source to be use
     * in the plan
     *
     * @return {@link Integer} that represent the size of the {@link java.util.Collection}
     */
    public abstract Integer getSizeSource();

    /**
     * return the UDF that will be use in the operators [2]  and [5]
     *
     * @param platformsTest the name of the branch that will be executed
     * @return The UDF that will be executed
     */
    public abstract FunctionDescriptor.SerializableFunction<Integer, Type2> getMapUDF(String platformsTest);

    /**
     * return an UDF that will be using in the filters of the {@link Type2}
     *
     * @return the Filter function
     */
    public PredicateDescriptor.SerializablePredicate<Type2> getFilterUDFType2(){
        return ele -> true;
    }

    /**
     * return an UDF that will be using the filters of {@link Integer}
     * @return The Filter Function
     */
    public PredicateDescriptor.SerializablePredicate<Integer> getFilterUDF(){
        return ele -> true;
    }

    /**
     * return an UDF that will be extract the key in the case of {@link Integer}
     *
     * @return the key extractor function of integer
     */
    public abstract FunctionDescriptor.SerializableFunction<Integer, Key> getKeyExtractorInteger();

    /**
     * return an UDF that will be extract the key in the case of {@link Type2}
     *
     * @return the key extractor function of type2
     */
    public abstract FunctionDescriptor.SerializableFunction<Type2, Key> getKeyExtractorType2();

}
