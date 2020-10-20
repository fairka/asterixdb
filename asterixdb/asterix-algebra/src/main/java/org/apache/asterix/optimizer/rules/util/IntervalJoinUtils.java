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

package org.apache.asterix.optimizer.rules.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.algebra.operators.physical.IntervalMergeJoinPOperator;
import org.apache.asterix.algebra.operators.physical.OverlappingIntervalPartitionJoinPOperator;
import org.apache.asterix.common.annotations.RangeAnnotation;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.runtime.operators.joins.interval.overlappingintervalpartition.OverlappingIntervalPartitionUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.AfterIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.BeforeIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.CoveredByIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.CoversIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.OverlappedByIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.OverlappingIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.OverlapsIntervalJoinUtilFactory;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RangeForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RangeForwardPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RangePartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty.PartitioningType;
import org.apache.hyracks.algebricks.core.algebra.properties.IntervalColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.rewriter.util.JoinUtils;
import org.apache.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.std.base.RangeId;

public class IntervalJoinUtils {

    private static final Logger LOGGER = Logger.getLogger(JoinUtils.class.getName());

    private static final Map<FunctionIdentifier, FunctionIdentifier> INTERVAL_JOIN_CONDITIONS = new HashMap<>();

    static {
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_AFTER, BuiltinFunctions.INTERVAL_BEFORE);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_BEFORE, BuiltinFunctions.INTERVAL_AFTER);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_COVERED_BY, BuiltinFunctions.INTERVAL_COVERS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_COVERS, BuiltinFunctions.INTERVAL_COVERED_BY);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_OVERLAPPED_BY, BuiltinFunctions.INTERVAL_OVERLAPS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_OVERLAPPING, BuiltinFunctions.INTERVAL_OVERLAPPING);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_OVERLAPS, BuiltinFunctions.INTERVAL_OVERLAPPED_BY);
    }

    protected static RangeAnnotation findRangeAnnotation(AbstractFunctionCallExpression fexp) {
        Iterator<IExpressionAnnotation> annotationIter = fexp.getAnnotations().values().iterator();
        while (annotationIter.hasNext()) {
            IExpressionAnnotation annotation = annotationIter.next();
            if (annotation instanceof RangeAnnotation) {
                return (RangeAnnotation) annotation;
            }
        }
        return null;
    }

    protected static void setSortMergeIntervalJoinOp(AbstractBinaryJoinOperator op, FunctionIdentifier fi,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, IOptimizationContext context,
            IntervalPartitions intervalPartitions) throws CompilationException {
        IIntervalJoinUtilFactory mjcf = createIntervalJoinUtilFactory(fi, intervalPartitions.getRangeMap());
        op.setPhysicalOperator(new IntervalMergeJoinPOperator(op.getJoinKind(),
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(), mjcf, intervalPartitions));
    }

    protected static void setOverlappingIntervalPartitionJoinOp(AbstractBinaryJoinOperator op, FunctionIdentifier fi,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, IOptimizationContext context,
            IntervalPartitions intervalPartitions, int left, int right) throws AlgebricksException {
        long leftCount = 10000;
        long rightCount = 10000;
        long leftMaxDuration = 100;
        long rightMaxDuration = 100;
        int tuplesPerFrame = 300;

        int k = OverlappingIntervalPartitionUtil.determineK(leftCount, leftMaxDuration, rightCount, rightMaxDuration,
                tuplesPerFrame);

        // Add two partition for intervals that start or end outside the given range.
        k += 2;
        if (k <= 2) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("IntervalPartitionJoin has overridden the suggested value of k (" + k + ") with 3.");
            }
            k = 3;
        }

        RangeId leftRangeId = context.newRangeId();
        RangeId rightRangeId = context.newRangeId();
        insertRangeForward(op, left, leftRangeId, intervalPartitions.getRangeMap(), context);
        insertRangeForward(op, right, rightRangeId, intervalPartitions.getRangeMap(), context);

        IIntervalJoinUtilFactory mjcf = createIntervalJoinUtilFactory(fi, intervalPartitions.getRangeMap());
        // Custom range exchange
//        insertRangeExchange(op, left, sideLeft, context, intervalPartitions);
//        insertRangeExchange(op, right, sideRight, context, intervalPartitions);
        // Custom range exchange END

        List<LogicalVariable> leftPartitionVar = Arrays.asList(context.newVar(), context.newVar());
        List<LogicalVariable> rightPartitionVar = Arrays.asList(context.newVar(), context.newVar());

        insertPartitionSortKey(op, left, leftPartitionVar, sideLeft.get(0), context);
        insertPartitionSortKey(op, right, rightPartitionVar, sideRight.get(0), context);

        insertOverlappingIntervalPartitionSortKey(op, left, sideLeft, leftPartitionVar.get(0), context, k, leftRangeId);
        insertOverlappingIntervalPartitionSortKey(op, right, sideRight, rightPartitionVar.get(0), context, k,
                rightRangeId);

        op.setPhysicalOperator(new OverlappingIntervalPartitionJoinPOperator(op.getJoinKind(),
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(), k, mjcf, leftPartitionVar,
                rightPartitionVar, intervalPartitions));
    }

    private static void insertRangeForward(AbstractBinaryJoinOperator op, int branch, RangeId rangeId,
            RangeMap rangeMap, IOptimizationContext context) throws AlgebricksException {
        RangeForwardOperator rfo = new RangeForwardOperator(rangeId, rangeMap);
        rfo.setExecutionMode(op.getExecutionMode());
        rfo.getInputs().add(op.getInputs().get(branch));
        RangeForwardPOperator rfpo = new RangeForwardPOperator(rangeId, rangeMap);
        rfo.setPhysicalOperator(rfpo);
        Mutable<ILogicalOperator> rfoRef = new MutableObject<>(rfo);
        op.getInputs().set(branch, rfoRef);

        context.computeAndSetTypeEnvironmentForOperator(rfo);
    }

    /**
     * Certain Relations not yet supported as seen below. Will default to regular join.
     * Inserts partition sort key.
     */
    protected static IntervalPartitions createIntervalPartitions(AbstractBinaryJoinOperator op, FunctionIdentifier fi,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, RangeMap rangeMap,
            IOptimizationContext context, int left, int right) throws AlgebricksException {

        List<LogicalVariable> leftPartitionVar = new ArrayList<>(2);
        leftPartitionVar.add(context.newVar());
        leftPartitionVar.add(context.newVar());
        List<LogicalVariable> rightPartitionVar = new ArrayList<>(2);
        rightPartitionVar.add(context.newVar());
        rightPartitionVar.add(context.newVar());

        insertPartitionSortKey(op, left, leftPartitionVar, sideLeft.get(0), context);
        insertPartitionSortKey(op, right, rightPartitionVar, sideRight.get(0), context);

        List<IntervalColumn> leftIC = Collections.singletonList(new IntervalColumn(leftPartitionVar.get(0),
                leftPartitionVar.get(1), OrderOperator.IOrder.OrderKind.ASC));
        List<IntervalColumn> rightIC = Collections.singletonList(new IntervalColumn(rightPartitionVar.get(0),
                rightPartitionVar.get(1), OrderOperator.IOrder.OrderKind.ASC));

        //Set Partitioning Types
        PartitioningType leftPartitioningType = PartitioningType.ORDERED_PARTITIONED;
        PartitioningType rightPartitioningType = PartitioningType.ORDERED_PARTITIONED;
        if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPED_BY)) {
            rightPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPS)) {
            leftPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPING)) {
            leftPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
            rightPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERS)) {
            leftPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERED_BY)) {
            rightPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_BEFORE)) {
            leftPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_FOLLOWING;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_AFTER)) {
            rightPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_FOLLOWING;
        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, fi.getName());
        }

        //Create Left and Right Local Order Column
        ArrayList<OrderColumn> leftLocalOrderColumn = new ArrayList<>();
        for (LogicalVariable v : sideLeft) {
            leftLocalOrderColumn.add(new OrderColumn(v, rightIC.get(0).getOrder()));
        }
        ArrayList<OrderColumn> rightLocalOrderColumn = new ArrayList<>();
        for (LogicalVariable v : sideRight) {
            rightLocalOrderColumn.add(new OrderColumn(v, rightIC.get(0).getOrder()));
        }
        return new IntervalPartitions(rangeMap, leftIC, rightIC, leftPartitioningType, rightPartitioningType,
                leftLocalOrderColumn, rightLocalOrderColumn);
    }

    protected static FunctionIdentifier isIntervalJoinCondition(ILogicalExpression e,
            Collection<LogicalVariable> inLeftAll, Collection<LogicalVariable> inRightAll,
            Collection<LogicalVariable> outLeftFields, Collection<LogicalVariable> outRightFields, int left,
            int right) {
        FunctionIdentifier fiReturn;
        boolean switchArguments = false;
        if (e.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
        FunctionIdentifier fi = fexp.getFunctionIdentifier();
        if (isIntervalFunction(fi)) {
            fiReturn = fi;
        } else {
            return null;
        }
        ILogicalExpression opLeft = fexp.getArguments().get(left).getValue();
        ILogicalExpression opRight = fexp.getArguments().get(right).getValue();
        if (opLeft.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || opRight.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return null;
        }
        LogicalVariable var1 = ((VariableReferenceExpression) opLeft).getVariableReference();
        if (inLeftAll.contains(var1) && !outLeftFields.contains(var1)) {
            outLeftFields.add(var1);
        } else if (inRightAll.contains(var1) && !outRightFields.contains(var1)) {
            outRightFields.add(var1);
            fiReturn = getInverseIntervalFunction(fi);
            switchArguments = true;
        } else {
            return null;
        }
        LogicalVariable var2 = ((VariableReferenceExpression) opRight).getVariableReference();
        if (inLeftAll.contains(var2) && !outLeftFields.contains(var2) && switchArguments) {
            outLeftFields.add(var2);
        } else if (inRightAll.contains(var2) && !outRightFields.contains(var2) && !switchArguments) {
            outRightFields.add(var2);
        } else {
            return null;
        }
        return fiReturn;
    }

    /**
     * Certain Relations not yet supported as seen below. Will default to regular join.
     */
    private static IIntervalJoinUtilFactory createIntervalJoinUtilFactory(FunctionIdentifier fi, RangeMap rangeMap)
            throws CompilationException {
        IIntervalJoinUtilFactory mjcf;
        if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPED_BY)) {
            mjcf = new OverlappedByIntervalJoinUtilFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPS)) {
            mjcf = new OverlapsIntervalJoinUtilFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERS)) {
            mjcf = new CoversIntervalJoinUtilFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERED_BY)) {
            mjcf = new CoveredByIntervalJoinUtilFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_BEFORE)) {
            mjcf = new BeforeIntervalJoinUtilFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_AFTER)) {
            mjcf = new AfterIntervalJoinUtilFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPING)) {
            mjcf = new OverlappingIntervalJoinUtilFactory(rangeMap);
        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, fi.getName());
        }
        return mjcf;
    }

    private static boolean isIntervalFunction(FunctionIdentifier fi) {
        return INTERVAL_JOIN_CONDITIONS.containsKey(fi);
    }

    private static FunctionIdentifier getInverseIntervalFunction(FunctionIdentifier fi) {
        return INTERVAL_JOIN_CONDITIONS.get(fi);
    }

    private static void insertPartitionSortKey(AbstractBinaryJoinOperator op, int branch,
            List<LogicalVariable> partitionVars, LogicalVariable intervalVar, IOptimizationContext context)
            throws AlgebricksException {
        Mutable<ILogicalExpression> intervalExp = new MutableObject<>(new VariableReferenceExpression(intervalVar));

        List<Mutable<ILogicalExpression>> assignExps = new ArrayList<>();
        // Start partition
        IFunctionInfo startFi = FunctionUtil.getFunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_START);
        ScalarFunctionCallExpression startPartitionExp = new ScalarFunctionCallExpression(startFi, intervalExp);
        assignExps.add(new MutableObject<>(startPartitionExp));
        // End partition
        IFunctionInfo endFi = FunctionUtil.getFunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_END);
        ScalarFunctionCallExpression endPartitionExp = new ScalarFunctionCallExpression(endFi, intervalExp);
        assignExps.add(new MutableObject<>(endPartitionExp));

        AssignOperator ao = new AssignOperator(partitionVars, assignExps);
        ao.setSourceLocation(op.getSourceLocation());
        ao.setExecutionMode(op.getExecutionMode());
        AssignPOperator apo = new AssignPOperator();
        ao.setPhysicalOperator(apo);
        Mutable<ILogicalOperator> aoRef = new MutableObject<>(ao);
        ao.getInputs().add(op.getInputs().get(branch));
        op.getInputs().set(branch, aoRef);

        context.computeAndSetTypeEnvironmentForOperator(ao);
    }

    private static void insertOverlappingIntervalPartitionSortKey(AbstractBinaryJoinOperator op, int branch,
            List<LogicalVariable> sortVars, LogicalVariable intervalVar, IOptimizationContext context, int k,
            RangeId rangeId) throws AlgebricksException {
        MutableObject<ILogicalExpression> intervalExp =
                new MutableObject<>(new VariableReferenceExpression(intervalVar));
        MutableObject<ILogicalExpression> rangeIdConstant =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(rangeId.getId()))));
        MutableObject<ILogicalExpression> kConstant =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(k))));

        List<Mutable<ILogicalExpression>> assignExps = new ArrayList<>();
        // Start partition
        IFunctionInfo startFi = FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.INTERVAL_PARTITION_JOIN_START);
        @SuppressWarnings("unchecked")
        ScalarFunctionCallExpression startPartitionExp =
                new ScalarFunctionCallExpression(startFi, intervalExp, rangeIdConstant, kConstant);
        assignExps.add(new MutableObject<ILogicalExpression>(startPartitionExp));
        // End partition
        IFunctionInfo endFi = FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.INTERVAL_PARTITION_JOIN_END);
        @SuppressWarnings("unchecked")
        ScalarFunctionCallExpression endPartitionExp =
                new ScalarFunctionCallExpression(endFi, intervalExp, rangeIdConstant, kConstant);
        assignExps.add(new MutableObject<ILogicalExpression>(endPartitionExp));

        AssignOperator ao = new AssignOperator(sortVars, assignExps);
        ao.setExecutionMode(op.getExecutionMode());
        AssignPOperator apo = new AssignPOperator();
        ao.setPhysicalOperator(apo);
        Mutable<ILogicalOperator> aoRef = new MutableObject<>(ao);
        ao.getInputs().add(op.getInputs().get(branch));
        op.getInputs().set(branch, aoRef);

        context.computeAndSetTypeEnvironmentForOperator(ao);
    }

    private static void insertRangeExchange(AbstractBinaryJoinOperator op, int branch, List<LogicalVariable> keys,
            IOptimizationContext context, IntervalPartitions intervalPartitions) throws AlgebricksException {
        ArrayList<OrderColumn> order = new ArrayList<>();
        for (LogicalVariable v : keys) {
            order.add(new OrderColumn(v, intervalPartitions.getLeftLocalOrderColumn().get(0).getOrder()));
        }
        IPhysicalOperator rpe = new RangePartitionExchangePOperator(order, context.getComputationNodeDomain(),
                intervalPartitions.getRangeMap());

        Mutable<ILogicalOperator> ci = op.getInputs().get(branch);
        ExchangeOperator exchg = new ExchangeOperator();
        exchg.setPhysicalOperator(rpe);
        setNewOp(ci, exchg, context);
        exchg.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        OperatorPropertiesUtil.computeSchemaAndPropertiesRecIfNull(exchg, context);
        context.computeAndSetTypeEnvironmentForOperator(exchg);
    }

    private static void setNewOp(Mutable<ILogicalOperator> opRef, AbstractLogicalOperator newOp,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator oldOp = opRef.getValue();
        opRef.setValue(newOp);
        newOp.getInputs().add(new MutableObject<ILogicalOperator>(oldOp));
        newOp.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(newOp);
        PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(newOp, context);
    }
}
