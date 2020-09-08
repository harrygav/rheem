package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.TableSink;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.spark.operators.SparkTableSink;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * Mapping from {@link TableSink} to {@link SparkTableSink}.
 */
public class TableSinkMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "sink",
                new TableSink(new Properties(),"", ""),
                false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<TableSink>(
                (matchedOperator, epoch) -> new SparkTableSink(matchedOperator).at(epoch)
        );
    }
}
