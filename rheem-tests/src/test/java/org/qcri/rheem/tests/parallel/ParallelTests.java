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
 * {@link ParallelTests} is an test that validate how a plan can be executed in parallel
 * using to differents engines
 *
 */
public class ParallelTests {


    @Test
    public void executionParallel(){

        ParallelPlan<Integer, Tuple4<Integer, Instant, Instant, String>> planInteger = new IntegerParallelPlan<Tuple4<Integer, Instant, Instant, String>, Integer>(ReflectionUtils.specify(Tuple4.class), Integer.class) {
            @Override
            public Integer getSizeSource() {
                return 10;
            }

            @Override
            public FunctionDescriptor.SerializableFunction<Integer, Tuple4<Integer, Instant, Instant, String>> getMapUDF(String platform) {
                return new FunctionWithTimerOfWait(platform);
            }

            @Override
            public FunctionDescriptor.SerializablePredicate<Tuple4<Integer, Instant, Instant, String>> getFilterUDFType2() {
                return ele -> true;
            }

            @Override
            public FunctionDescriptor.SerializablePredicate<Integer> getFilterUDF() {
                return ele -> true;
            }

            @Override
            public FunctionDescriptor.SerializableFunction<Integer, Integer> getKeyExtractorInteger() {
                return ele -> ele;
            }

            @Override
            public FunctionDescriptor.SerializableFunction<Tuple4<Integer, Instant, Instant, String>, Integer> getKeyExtractorType2() {
                return tuple -> tuple._1();
            }
        };

        RheemContext context = planInteger.getRheemContext();

        RheemPlan plan = planInteger.createPlan();

        context.execute(plan);

        Collection<Tuple2<Tuple4<Integer, Instant, Instant, String>, Tuple4<Integer, Instant, Instant, String>>> list =
            ((Collection<Tuple2<Tuple4<Integer, Instant, Instant, String>, Tuple4<Integer, Instant, Instant, String>>>) planInteger.getOutput());


        //TODO validate
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


//        System.out.println("min_left_exec: "+min_left_exec);
//        System.out.println("max_left_exec: "+max_left_exec);
//        System.out.println("min_right_exec: "+min_right_exec);
//        System.out.println("max_right_exec: "+max_right_exec);
//        System.out.println("min_left_fun: "+min_left_fun);
//        System.out.println("max_left_fun: "+max_left_fun);
//        System.out.println("min_right_fun: "+min_right_fun);
//        System.out.println("max_right_fun: "+max_right_fun);

        Tuple2<Instant, Instant> range_left = makeRange(min_left_exec, min_left_fun, max_left_exec, max_left_fun);
        Tuple2<Instant, Instant> range_right = makeRange(min_right_exec, min_right_fun, max_right_exec, max_right_fun);
//        System.out.println(range_left);
//        System.out.println(range_right);

        Assert.assertTrue(
            (
                ( range_left.field0.isBefore( range_right.field1 ) )
                    &&
                    ( range_left.field1.isAfter( range_right.field0 ) )
            )
        );
    }

    private Instant minInstant(Instant instant1, Instant instant2){
        return (instant1.isBefore(instant2))? instant1: instant2;
    }

    private Instant maxInstant(Instant instant1, Instant instant2){
        return (instant1.isAfter(instant2))? instant1: instant2;
    }

    private Tuple2<Instant, Instant> makeRange(Instant min_exec, Instant min_fun, Instant max_exec, Instant max_fun){
        return new Tuple2<>(
            minInstant(min_exec, min_fun),
            maxInstant(max_exec, max_fun)
        );
    }
}
