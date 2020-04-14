package org.qcri.rheem.tests.parallel;

import org.qcri.rheem.core.function.FunctionDescriptor;
import scala.Tuple4;

import java.time.Instant;

/**
 * {@link FunctionWithTimerOfWait} is a {@link FunctionDescriptor.SerializableFunction}
 * which create delay and also create a timestamp as label in the tuples that are executed
 */
public class FunctionWithTimerOfWait implements FunctionDescriptor.SerializableFunction<Integer, Tuple4<Integer, Instant, Instant, String>> {

    /**
     * starting describe the moment when the first call is executed
     */
    Instant starting = null;

    /**
     * platform_name is the name that will describe the branch where this instance is executed
     */
    String platform_name = "";

    /**
     * Constructor of {@link FunctionWithTimerOfWait}
     * @param platform_name name of the branch where is executed in the plan
     */
    public FunctionWithTimerOfWait(String platform_name) {
        this.platform_name = platform_name;
    }

    /**
     * Apply allow the labeling of elements, and also create a delay of one second per tuple.
     *
     * @param ele is the element that will be labeled
     * @return the tuple with the labels and timestamps
     */
    @Override
    public Tuple4<Integer, Instant, Instant, String> apply(Integer ele) {
        if(starting == null){
            starting = Instant.now();
        }
        long delay = Instant.now().toEpochMilli();
        while(Instant.now().toEpochMilli() - delay < 1000);
        return new Tuple4<>(ele, starting, Instant.now(), platform_name);
    }
}
