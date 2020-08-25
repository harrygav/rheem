package org.qcri.rheem.hbase.mapping;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.hbase.operators.HBaseFilterOperator;
import org.qcri.rheem.hbase.platform.HBasePlatform;

import java.util.Collection;
import java.util.Collections;


/**
 * Mapping from {@link FilterOperator} to {@link HBaseFilterOperator}.
 */
@SuppressWarnings("unchecked")
public class FilterMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                HBasePlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern<FilterOperator<Record>> operatorPattern = new OperatorPattern<>(
                "filter", new FilterOperator<>(null, DataSetType.createDefault(Record.class)),
                false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<FilterOperator>(
                (matchedOperator, epoch) -> new HBaseFilterOperator(matchedOperator).at(epoch)
        );
    }
}
