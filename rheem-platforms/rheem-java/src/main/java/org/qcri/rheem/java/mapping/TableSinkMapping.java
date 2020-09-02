package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.TableSink;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.java.operators.JavaTableSink;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * Mapping from {@link TableSink} to {@link JavaTableSink}.
 */
public class TableSinkMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "sink",
                new TableSink(new Properties(), ""),
                false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<TableSink>(
                (matchedOperator, epoch) -> new JavaTableSink(matchedOperator).at(epoch)
        );
    }
}
