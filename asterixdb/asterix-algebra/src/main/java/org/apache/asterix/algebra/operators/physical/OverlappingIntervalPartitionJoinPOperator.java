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
package org.apache.asterix.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.optimizer.rules.util.IntervalPartitions;
import org.apache.asterix.runtime.operators.joins.interval.overlappingintervalpartition.OverlappingIntervalPartitionJoinOperatorDescriptor;
import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtilFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;

public class OverlappingIntervalPartitionJoinPOperator extends AbstractJoinPOperator {
    private static final int START = 0;
    private static final int END = 1;

    private final int memSizeInFrames;
    private final int k;
    private final List<LogicalVariable> leftPartitionVar;
    private final List<LogicalVariable> rightPartitionVar;
    private final List<LogicalVariable> keysLeftBranch;
    private final List<LogicalVariable> keysRightBranch;
    protected final IIntervalJoinUtilFactory mjcf;
    protected final IntervalPartitions intervalPartitions;

    private static final Logger LOGGER = Logger.getLogger(OverlappingIntervalPartitionJoinPOperator.class.getName());

    public OverlappingIntervalPartitionJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeftOfEqualities, List<LogicalVariable> sideRightOfEqualities,
            int memSizeInFrames, int k, IIntervalJoinUtilFactory mjcf, List<LogicalVariable> leftPartitionVar,
            List<LogicalVariable> rightPartitionVar, IntervalPartitions intervalPartitions) {
        super(kind, partitioningType);
        this.memSizeInFrames = memSizeInFrames;
        this.leftPartitionVar = leftPartitionVar;
        this.rightPartitionVar = rightPartitionVar;
        this.keysLeftBranch = sideLeftOfEqualities;
        this.keysRightBranch = sideRightOfEqualities;
        this.intervalPartitions = intervalPartitions;
        this.mjcf = mjcf;
        this.k = k;

        LOGGER.fine("IntervalPartitionJoinPOperator constructed with: JoinKind=" + kind + ", JoinPartitioningType="
                + partitioningType + ", List<LogicalVariable>=" + sideLeftOfEqualities + ", List<LogicalVariable>="
                + sideRightOfEqualities + ", int memSizeInFrames=" + memSizeInFrames + ", int k=" + k
                + ", IMergeJoinCheckerFactory mjcf=" + mjcf + ".");
    }

    public int getK() {
        return k;

    }

    public List<LogicalVariable> getLeftPartitionVar() {
        return leftPartitionVar;
    }

    public List<LogicalVariable> getRightPartitionVar() {
        return rightPartitionVar;
    }

    public List<LogicalVariable> getKeysLeftBranch() {
        return keysLeftBranch;
    }

    public List<LogicalVariable> getKeysRightBranch() {
        return keysRightBranch;
    }

    public IIntervalJoinUtilFactory getIntervalMergeJoinCheckerFactory() {
        return mjcf;
    }

    public String getIntervalJoin() {
        return "OVERLAPPING_INTERVAL_PARTITION_JOIN";
    }

    IOperatorDescriptor getIntervalOperatorDescriptor(int[] keysLeft, int[] keysRight, IOperatorDescriptorRegistry spec,
            RecordDescriptor recordDescriptor, IIntervalJoinUtilFactory mjcf) {
        return new OverlappingIntervalPartitionJoinOperatorDescriptor(spec, memSizeInFrames, k, keysLeft, keysRight,
                recordDescriptor, mjcf, intervalPartitions.getRangeMap());
    }

    //Used to be Extension Operator
    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.DELEGATE_OPERATOR;
    }

    @Override
    public String toString() {
        return getIntervalJoin() + " " + keysLeftBranch + " " + keysRightBranch;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator iop, IOptimizationContext context) {
        IPartitioningProperty pp = new OrderedPartitionedProperty(intervalPartitions.getLeftLocalOrderColumn(), null,
                intervalPartitions.getRangeMap());
        List<ILocalStructuralProperty> propsLocal = new ArrayList<>();
        propsLocal.add(new LocalOrderProperty(getLeftLocalSortOrderColumn()));
        deliveredProperties = new StructuralPropertiesVector(pp, propsLocal);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator iop,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[2];
        AbstractLogicalOperator op = (AbstractLogicalOperator) iop;

        IPartitioningProperty ppLeft = null;
        List<ILocalStructuralProperty> ispLeft = new ArrayList<>();
        ispLeft.add(new LocalOrderProperty(getLeftLocalSortOrderColumn()));

        IPartitioningProperty ppRight = null;
        List<ILocalStructuralProperty> ispRight = new ArrayList<>();
        ispRight.add(new LocalOrderProperty(getRightLocalSortOrderColumn()));

        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            ppLeft = new OrderedPartitionedProperty(intervalPartitions.getLeftLocalOrderColumn(), null,
                    intervalPartitions.getRangeMap());
            ppRight = new OrderedPartitionedProperty(intervalPartitions.getRightLocalOrderColumn(), null,
                    intervalPartitions.getRangeMap());
        }

        pv[0] = new StructuralPropertiesVector(ppLeft, ispLeft);
        pv[1] = new StructuralPropertiesVector(ppRight, ispRight);
        IPartitioningRequirementsCoordinator prc = IPartitioningRequirementsCoordinator.NO_COORDINATION;
        return new PhysicalRequirements(pv, prc);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        int[] keysLeft = JobGenHelper.variablesToFieldIndexes(keysLeftBranch, inputSchemas[0]);
        int[] keysRight = JobGenHelper.variablesToFieldIndexes(keysRightBranch, inputSchemas[1]);

        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        RecordDescriptor recordDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);

        IOperatorDescriptor opDesc = getIntervalOperatorDescriptor(keysLeft, keysRight, spec, recordDescriptor, mjcf);
        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);

        ILogicalOperator src1 = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src1, 0, op, 0);
        ILogicalOperator src2 = op.getInputs().get(1).getValue();
        builder.contributeGraphEdge(src2, 0, op, 1);
    }

    protected ArrayList<OrderColumn> getLeftLocalSortOrderColumn() {
        ArrayList<OrderColumn> order = new ArrayList<>();
        switch (intervalPartitions.getLeftIntervalColumn().get(0).getOrder()) {
            case ASC:
                order.add(new OrderColumn(leftPartitionVar.get(END), OrderKind.ASC));
                order.add(new OrderColumn(leftPartitionVar.get(START), OrderKind.DESC));
            case DESC:
                order.add(new OrderColumn(leftPartitionVar.get(START), OrderKind.ASC));
                order.add(new OrderColumn(leftPartitionVar.get(END), OrderKind.DESC));
        }
        return order;
    }

    protected ArrayList<OrderColumn> getRightLocalSortOrderColumn() {
        ArrayList<OrderColumn> order = new ArrayList<>();
        switch (intervalPartitions.getRightIntervalColumn().get(0).getOrder()) {
            case ASC:
                order.add(new OrderColumn(rightPartitionVar.get(END), OrderKind.ASC));
                order.add(new OrderColumn(rightPartitionVar.get(START), OrderKind.DESC));
            case DESC:
                order.add(new OrderColumn(rightPartitionVar.get(START), OrderKind.ASC));
                order.add(new OrderColumn(rightPartitionVar.get(END), OrderKind.DESC));
        }
        return order;
    }
}