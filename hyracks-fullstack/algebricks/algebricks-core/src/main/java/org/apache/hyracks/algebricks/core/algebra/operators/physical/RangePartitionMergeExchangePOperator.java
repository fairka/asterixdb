/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder.TargetConstraint;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty.PropertyType;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.data.partition.range.FieldRangePartitionComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;

public final class RangePartitionMergeExchangePOperator extends AbstractRangeExchangePOperator {

    public RangePartitionMergeExchangePOperator(List<OrderColumn> partitioningFields, INodeDomain domain,
            RangeMap rangeMap) {
        super(partitioningFields, domain, rangeMap);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.RANGE_PARTITION_MERGE_EXCHANGE;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        IPartitioningProperty pp =
                new OrderedPartitionedProperty(new ArrayList<>(partitioningFields), domain, rangeMap);
        List<ILocalStructuralProperty> locals = computeDeliveredLocalProperties(op);
        this.deliveredProperties = new StructuralPropertiesVector(pp, locals);
    }

    private List<ILocalStructuralProperty> computeDeliveredLocalProperties(ILogicalOperator op) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        List<ILocalStructuralProperty> op2Locals = op2.getDeliveredPhysicalProperties().getLocalProperties();
        List<ILocalStructuralProperty> locals = new ArrayList<>();
        for (ILocalStructuralProperty prop : op2Locals) {
            if (prop.getPropertyType() == PropertyType.LOCAL_ORDER_PROPERTY) {
                locals.add(prop);
            } else {
                break;
            }
        }
        return locals;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        List<ILocalStructuralProperty> orderProps = new LinkedList<>();
        List<OrderColumn> columns = new ArrayList<>();
        for (OrderColumn oc : partitioningFields) {
            LogicalVariable var = oc.getColumn();
            columns.add(new OrderColumn(var, oc.getOrder()));
        }
        orderProps.add(new LocalOrderProperty(columns));
        StructuralPropertiesVector[] r =
                new StructuralPropertiesVector[] { new StructuralPropertiesVector(null, orderProps) };
        return new PhysicalRequirements(r, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public Pair<IConnectorDescriptor, TargetConstraint> createConnectorDescriptor(IConnectorDescriptorRegistry spec,
            ILogicalOperator op, IOperatorSchema opSchema, JobGenContext context) throws AlgebricksException {
        Triple<int[], IBinaryComparatorFactory[], INormalizedKeyComputerFactory> pOrderColumns =
                createOrderColumnsAndComparatorsWithNormKeyComputer(op, opSchema, context);
        int[] sortFields = pOrderColumns.first;
        IBinaryComparatorFactory[] comps = pOrderColumns.second;
        INormalizedKeyComputerFactory nkcf = pOrderColumns.third;
        ITuplePartitionComputerFactory tpcf = new FieldRangePartitionComputerFactory(sortFields, comps,
                crateRangeMapSupplier(), op.getSourceLocation());
        IConnectorDescriptor conn = new MToNPartitioningMergingConnectorDescriptor(spec, tpcf, sortFields, comps, nkcf);
        return new Pair<>(conn, null);
    }
}