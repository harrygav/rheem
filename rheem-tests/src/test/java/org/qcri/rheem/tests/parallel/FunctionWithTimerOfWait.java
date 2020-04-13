package org.qcri.rheem.tests.parallel;

import org.qcri.rheem.core.function.FunctionDescriptor;
import scala.Tuple4;

import java.time.Instant;

/**
 * TODO ADD doc
 */
public class FunctionWithTimerOfWait implements FunctionDescriptor.SerializableFunction<Integer, Tuple4<Integer, Instant, Instant, String>> {

    /**
     * TODO ADD doc
     */
    Instant starting = null;

    /**
     * TODO ADD doc
     */
    String platform_name = "";

    /**
     * TODO ADD doc
     * @param platform_name
     */
    public FunctionWithTimerOfWait(String platform_name) {
        this.platform_name = platform_name;
    }

    /**
     * TODO add doc
     * @param ele
     * @return
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
