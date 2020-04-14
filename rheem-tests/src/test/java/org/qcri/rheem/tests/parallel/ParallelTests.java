package org.qcri.rheem.tests.parallel;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.ReflectionUtils;
import scala.Tuple4;

import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;

/**
 * {@link ParallelTests} is a test that validates how a plan can be executed in parallel
 * using to differents engines
 *
 */
public class ParallelTests {


    /**
     * {@link ParallelTests#executionParallel()} in the test that validate the plan bellow:
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
     * the validation is get done by the intersection of the timestamps to validate
     * if both stage it was label the data during the same period
     *
     */
    @Test
    public void executionParallel(){

        ParallelPlan<Integer, Tuple4<Integer, Instant, Instant, String>> planInteger =
            new IntegerParallelPlan<Tuple4<Integer, Instant, Instant, String>, Integer>(
                ReflectionUtils.specify(Tuple4.class),
                Integer.class
            ) {
                /**
                 * @see IntegerParallelPlan#getSizeSource()
                 */
                @Override
                public Integer getSizeSource() {
                    return 10;
                }

                /**
                 * @see IntegerParallelPlan#getMapUDF(String)
                 */
                @Override
                public FunctionDescriptor.SerializableFunction<Integer, Tuple4<Integer, Instant, Instant, String>> getMapUDF(String platform) {
                    return new FunctionWithTimerOfWait(platform);
                }


                /**
                 * @see IntegerParallelPlan#getKeyExtractorInteger()
                 */
                @Override
                public FunctionDescriptor.SerializableFunction<Integer, Integer> getKeyExtractorInteger() {
                    return ele -> ele;
                }

                /**
                 * @see IntegerParallelPlan#getKeyExtractorType2()
                 */
                @Override
                public FunctionDescriptor.SerializableFunction<Tuple4<Integer, Instant, Instant, String>, Integer> getKeyExtractorType2() {
                    return tuple -> tuple._1();
                }
            }
        ;

        //Execution of the plan make it with the functions described.
        RheemContext context = planInteger.getRheemContext();
        RheemPlan plan = planInteger.createPlan();
        context.execute(plan);

        Collection<Tuple2<Tuple4<Integer, Instant, Instant, String>, Tuple4<Integer, Instant, Instant, String>>> list =
            (Collection<Tuple2<Tuple4<Integer, Instant, Instant, String>, Tuple4<Integer, Instant, Instant, String>>>)
                planInteger.getOutput()
        ;

        //recovery of the elements and try to find the min and max of the all the tuples
        Iterator<Tuple2<Tuple4<Integer, Instant, Instant, String>, Tuple4<Integer, Instant, Instant, String>>> iterator = list.iterator();
        Tuple2<Tuple4<Integer, Instant, Instant, String>, Tuple4<Integer, Instant, Instant, String>> first = iterator.next();

        //left is field0 and right is field1
        Instant min_left_exec = first.field0._3();
        Instant max_left_exec = first.field0._3();
        Instant min_right_exec = first.field1._3();
        Instant max_right_exec = first.field1._3();

        Instant min_left_fun = first.field0._2();
        Instant max_left_fun = first.field0._2();
        Instant min_right_fun = first.field1._2();
        Instant max_right_fun = first.field1._2();

        //Update the min and max
        while(iterator.hasNext()){
            Tuple2<Tuple4<Integer, Instant, Instant, String>, Tuple4<Integer, Instant, Instant, String>> tmp = iterator.next();
            if(min_left_exec.isAfter(tmp.field0._3())){
                min_left_exec = tmp.field0._3();
            }
            if(max_left_exec.isBefore(tmp.field0._3())){
                max_left_exec = tmp.field0._3();
            }
            if(min_right_exec.isAfter(tmp.field1._3())){
                min_right_exec = tmp.field1._3();
            }
            if(max_right_exec.isBefore(tmp.field1._3())){
                max_right_exec = tmp.field1._3();
            }

            if(min_left_fun.isAfter(tmp.field0._2())){
                min_left_fun = tmp.field0._2();
            }
            if(max_left_fun.isBefore(tmp.field0._2())){
                max_left_fun = tmp.field0._2();
            }
            if(min_right_fun.isAfter(tmp.field1._2())){
                min_right_fun = tmp.field1._2();
            }
            if(max_right_fun.isBefore(tmp.field1._2())){
                max_right_fun = tmp.field1._2();
            }
        }

        //Take the real range of the execution
        Tuple2<Instant, Instant> range_left = makeRange(min_left_exec, min_left_fun, max_left_exec, max_left_fun);
        Tuple2<Instant, Instant> range_right = makeRange(min_right_exec, min_right_fun, max_right_exec, max_right_fun);

        // assert the both range are overlapping
        Assert.assertTrue( isOverlapping(range_left, range_right) );
    }

    /**
     * return the min between to {@link Instant}
     *
     * @param instant1 {@link Instant} candidate to be the min between both
     * @param instant2 {@link Instant} candidate to be the min between both
     *
     * @return {@link Instant} that is the min value
     */
    private Instant minInstant(Instant instant1, Instant instant2){
        return (instant1.isBefore(instant2))? instant1: instant2;
    }

    /**
     * return the max between to {@link Instant}
     *
     * @param instant1 {@link Instant} candidate to be the max between both
     * @param instant2 {@link Instant} candidate to be the max between both
     *
     * @return {@link Instant} that is the max value
     */
    private Instant maxInstant(Instant instant1, Instant instant2){
        return (instant1.isAfter(instant2))? instant1: instant2;
    }

    /**
     * Given the elements make a range with them considering the higher
     * number will be the end of the range, and smaller will be the
     * starting point of the range
     *
     * @param min_exec candidate to be the starting point of the range
     * @param min_fun candidate to be the starting point of the range
     * @param max_exec candidate to be the ending point of the range
     * @param max_fun candidate to be the ending point of the range
     *
     * @return Range of the instant
     */
    private Tuple2<Instant, Instant> makeRange(Instant min_exec, Instant min_fun, Instant max_exec, Instant max_fun){
        return new Tuple2<>(
            minInstant(min_exec, min_fun),
            maxInstant(max_exec, max_fun)
        );
    }

    /**
     * this validate if two range of instance are overlapping or not
     *
     * @param range1 range of instance where the first is min and the second is the max
     * @param range2 range of instance where the first is min and the second is the max
     *
     * @return if both ranges are overlapping between them
     */
    private boolean isOverlapping(Tuple2<Instant, Instant> range1, Tuple2<Instant, Instant> range2){
        return (
            ( range1.field0.isBefore( range2.field1 ) )
                &&
                ( range1.field1.isAfter( range2.field0 ) )
        );
    }

}
