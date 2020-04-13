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
 * TODO: add doc
 */
public abstract class IntegerParallelPlan<Type2, Key> extends ParallelPlan<Integer, Type2> {

    /**
     * TODO: add doc
     */
    protected final Class<Key> keyClass;

    /**
     * TODO: add doc
     */
    protected Object output;

    /**
     * TODO: add doc
     */
    public IntegerParallelPlan(Class<Type2> type2Class, Class<Key> keyClass) {
        super(Integer.class, type2Class, 0, i -> i + 1);
        this.keyClass = keyClass;
    }

    /**
     * TODO: ADD Doc
     * @return
     */
    public PlatformsTest getLeftPlatform(){
        return PlatformsTest.Java;
    }

    /**
     * TODO: ADD Doc
     * @return
     */
    public PlatformsTest getRightPlatform(){
        return PlatformsTest.Spark;
    }


    /**
     * TODO: add doc
     * @return
     */
    public Object getOutput() {
        return output;
    }

    /**
     * TODO add doc
     * @param output
     * @return
     */
    private IntegerParallelPlan<Type2, Key> setOutput(Object output) {
        this.output = output;
        return this;
    }

    /**
     * TODO: add doc
     * @return
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

    public abstract Integer getSizeSource();

    public abstract FunctionDescriptor.SerializableFunction<Integer, Type2> getMapUDF(String platformsTest);

    public abstract PredicateDescriptor.SerializablePredicate<Type2> getFilterUDFType2();

    public abstract PredicateDescriptor.SerializablePredicate<Integer> getFilterUDF();

    public abstract FunctionDescriptor.SerializableFunction<Integer, Key> getKeyExtractorInteger();

    public abstract FunctionDescriptor.SerializableFunction<Type2, Key> getKeyExtractorType2();


}
