package org.qcri.rheem.tests.parallel;

import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.tests.util.PlatformsTest;

import java.util.Collection;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TODO: add doc
 * @param <Type1>
 * @param <Type2>
 */
public abstract class ParallelPlan<Type1, Type2> {

    /**
     * TODO: add doc
     */
    protected final Class<Type1> type1Class;

    protected final Class<Type2> type2Class;

    /**
     * TODO: add doc
     */
    protected final Type1 seed;

    /**
     * TODO: add doc
     */
    protected final UnaryOperator<Type1> generator;

    protected RheemContext rheemContext;

    /**
     * TODO: add doc
     * @param typeClass
     * @param type2Class
     * @param seed
     * @param generator
     */
    public ParallelPlan(Class<Type1> typeClass, Class<Type2> type2Class, Type1 seed, UnaryOperator<Type1> generator) {
        this.type1Class = typeClass;
        this.type2Class = type2Class;
        this.seed = seed;
        this.generator = generator;
        this.setVariables();
        Configuration conf = new Configuration();
        conf.setProperty("rheem.core.optimizer.enumeration.parallel-tasks", "true");
        this.rheemContext = new RheemContext(conf);

        this.rheemContext.register(Java.basicPlugin());
        this.rheemContext.register(Spark.basicPlugin());
    }

    /**
     * Generate a collection of lenght equal the size {@code size}, currently the implementation
     * is an correlative starting from {@code 0}
     *
     * @param size is the lenght of the collection that will be generated
     * @return {@link Collection} of {@link Type1} of the size described in the parameters
     */
    protected Collection<Type1> generateCollectionSource(long size) {
        return Stream.iterate(this.seed, this.generator).limit(size).collect(Collectors.toList());
    }

    /**
     * TODO: add doc
     * @param size
     * @param platformsTest
     * @return
     */
    protected CollectionSource<Type1> generateSource(long size, PlatformsTest platformsTest) {
        return this.generateSource(
            this.generateCollectionSource(size),
            platformsTest
        );
    }

    /**
     * TODO: add doc
     * generate the Source that will be base to pick the platform
     *
     * @param source
     * @return
     */
    protected CollectionSource<Type1> generateSource(Collection<Type1> source, PlatformsTest platformsTest) {
        CollectionSource<Type1> source_base = new CollectionSource<Type1>(source, this.type1Class);
        source_base.addTargetPlatform(platformsTest.getPlatform());
        return source_base;
    }

    /**
     * TODO: add doc
     * @param udf
     * @param platformsTest
     * @return
     */
    protected FilterOperator<Type1> generateFilterType1(PredicateDescriptor.SerializablePredicate<Type1> udf, PlatformsTest platformsTest) {
        FilterOperator<Type1> filter_base = new FilterOperator<Type1>(udf, this.type1Class);
        filter_base.addTargetPlatform(platformsTest.getPlatform());
        return filter_base;
    }

    /**
     * TODO: add doc
     * @param udf
     * @param platformsTest
     * @return
     */
    protected FilterOperator<Type2> generateFilterType2(PredicateDescriptor.SerializablePredicate<Type2> udf, PlatformsTest platformsTest) {
        FilterOperator<Type2> filter_base = new FilterOperator<Type2>(udf, this.type2Class);
        filter_base.addTargetPlatform(platformsTest.getPlatform());
        return filter_base;
    }

    /**
     * TODO: add doc
     * @param udf
     * @param platformsTest
     * @return
     */
    protected MapOperator<Type1, Type2> generateMapType1_2(FunctionDescriptor.SerializableFunction<Type1, Type2> udf, PlatformsTest platformsTest){
        return this.generateMap(
            udf,
            this.type1Class,
            this.type2Class,
            platformsTest
        );
    }

    protected <Input, Output> MapOperator<Input, Output> generateMap(
        FunctionDescriptor.SerializableFunction<Input, Output> udf,
        Class<Input> inputClass,
        Class<Output> outputClass,
        PlatformsTest platformsTest
    ){
        MapOperator<Input, Output> map_base = new MapOperator<Input, Output>(udf, inputClass, outputClass);
        map_base.addTargetPlatform(platformsTest.getPlatform());
        return map_base;
    }

        /**
         * TODO: add doc
         * @param keyExtractor1
         * @param keyExtractor2
         * @param keyClass
         * @param platformsTest
         * @param <Key>
         * @return
         */
    protected <Key> JoinOperator<Type1, Type1, Key> generateJoinType1(
        FunctionDescriptor.SerializableFunction<Type1, Key> keyExtractor1,
        FunctionDescriptor.SerializableFunction<Type1, Key> keyExtractor2,
        Class<Key> keyClass,
        PlatformsTest platformsTest
    ){
        return this.generateJoin(
            keyExtractor1,
            keyExtractor2,
            this.type1Class,
            this.type1Class,
            keyClass,
            platformsTest
        );
    }

    /**
     * TODO: add doc
     * @param keyExtractor1
     * @param keyExtractor2
     * @param keyClass
     * @param platformsTest
     * @param <Key>
     * @return
     */
    protected <Key> JoinOperator<Type2, Type2, Key> generateJoinType2(
        FunctionDescriptor.SerializableFunction<Type2, Key> keyExtractor1,
        FunctionDescriptor.SerializableFunction<Type2, Key> keyExtractor2,
        Class<Key> keyClass,
        PlatformsTest platformsTest
    ){
        return this.generateJoin(
            keyExtractor1,
            keyExtractor2,
            this.type2Class,
            this.type2Class,
            keyClass,
            platformsTest
        );
    }

    /**
     * TODO: add doc
     * @param keyExtractor1
     * @param keyExtractor2
     * @param keyClass
     * @param platformsTest
     * @param <Key>
     * @return
     */
    protected <Key> JoinOperator<Type1, Type2, Key> generateJoinType1_2(
        FunctionDescriptor.SerializableFunction<Type1, Key> keyExtractor1,
        FunctionDescriptor.SerializableFunction<Type2, Key> keyExtractor2,
        Class<Key> keyClass,
        PlatformsTest platformsTest
    ){
        return this.generateJoin(
            keyExtractor1,
            keyExtractor2,
            this.type1Class,
            this.type2Class,
            keyClass,
            platformsTest
        );
    }

    /**
     * TODO: add doc
     * @param keyExtractor1
     * @param keyExtractor2
     * @param keyClass
     * @param platformsTest
     * @param <Key>
     * @return
     */
    protected <Key> JoinOperator<Type2, Type1, Key> generateJoinType2_1(
        FunctionDescriptor.SerializableFunction<Type2, Key> keyExtractor1,
        FunctionDescriptor.SerializableFunction<Type1, Key> keyExtractor2,
        Class<Key> keyClass,
        PlatformsTest platformsTest
    ){
        return this.generateJoin(
            keyExtractor1,
            keyExtractor2,
            this.type2Class,
            this.type1Class,
            keyClass,
            platformsTest
        );
    }


    /**
     * TODO: add doc
     * @param keyExtractor1
     * @param keyExtractor2
     * @param t1Class
     * @param t2Class
     * @param keyClass
     * @param platformsTest
     * @param <T1>
     * @param <T2>
     * @param <Key>
     * @return
     */
    private <T1, T2, Key> JoinOperator<T1, T2, Key> generateJoin(
        FunctionDescriptor.SerializableFunction<T1, Key> keyExtractor1,
        FunctionDescriptor.SerializableFunction<T2, Key> keyExtractor2,
        Class<T1> t1Class,
        Class<T2> t2Class,
        Class<Key> keyClass,
        PlatformsTest platformsTest
    ){
        JoinOperator<T1, T2, Key> join_base = new JoinOperator<T1, T2, Key>(
            keyExtractor1,
            keyExtractor2,
            t1Class,
            t2Class,
            keyClass
        );
        join_base.addTargetPlatform(platformsTest.getPlatform());
        return join_base;
    }

    /**
     * TODO add doc
     * @param collector
     * @param outputType
     * @param platformsTest
     * @param <Output>
     * @return
     */
    protected <Output> LocalCallbackSink<Output> generateSink(Collection<Output> collector, DataSetType<Output> outputType, PlatformsTest platformsTest){
        LocalCallbackSink<Output> sink_base = LocalCallbackSink.createCollectingSink(collector, outputType);
        sink_base.addTargetPlatform(platformsTest.getPlatform());
        return sink_base;
    }


    /**
     * TODO add doc
     */
    protected void setVariables(){
        System.setProperty("rheem.core.optimizer.enumeration.parallel-tasks", "true");
    }

    /**
     * TODO add doc
     * @return
     */
    public RheemContext getRheemContext() {
        return rheemContext;
    }

    /**
     * create a plan that can be use in the execution of the parallel, because
     * have to branch on in java and the other in spark, but both of them can
     * be executed in parallel
     *
     * @return {@link RheemPlan} which will be executed in parallel mode, this contains
     * a join where the branch will be excuted in two independes platforms.
     */
    public abstract RheemPlan createPlan();

    /**
     * TODO add doc
     * @return
     */
    public abstract Object getOutput();
}
