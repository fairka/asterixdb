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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.algebra.operators.physical.IntervalForwardScanJoinPOperator;
import org.apache.asterix.common.annotations.RangeAnnotation;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.runtime.operators.joins.AfterIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.BeforeIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.CoveredByIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.CoversIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.EndedByIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.EndsIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.MeetsIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.MetByIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.OverlappedByIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.OverlappingIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.OverlapsIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.StartedByIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.StartsIntervalMergeJoinCheckerFactory;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty.PartitioningType;
import org.apache.hyracks.algebricks.core.algebra.properties.IntervalColumn;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

public class IntervalJoinUtils {

    private static final int LEFT = 0;
    private static final int RIGHT = 1;

    private static final Map<FunctionIdentifier, FunctionIdentifier> INTERVAL_JOIN_CONDITIONS = new HashMap<>();

    IntervalJoinUtils() {
    }

    static {
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_AFTER, BuiltinFunctions.INTERVAL_BEFORE);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_BEFORE, BuiltinFunctions.INTERVAL_AFTER);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_COVERED_BY, BuiltinFunctions.INTERVAL_COVERS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_COVERS, BuiltinFunctions.INTERVAL_COVERED_BY);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_ENDED_BY, BuiltinFunctions.INTERVAL_ENDS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_ENDS, BuiltinFunctions.INTERVAL_ENDED_BY);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_MEETS, BuiltinFunctions.INTERVAL_MET_BY);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_MET_BY, BuiltinFunctions.INTERVAL_MEETS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_OVERLAPPED_BY, BuiltinFunctions.INTERVAL_OVERLAPS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_OVERLAPPING, BuiltinFunctions.INTERVAL_OVERLAPPING);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_OVERLAPS, BuiltinFunctions.INTERVAL_OVERLAPPED_BY);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_STARTED_BY, BuiltinFunctions.INTERVAL_STARTS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_STARTS, BuiltinFunctions.INTERVAL_STARTED_BY);
    }

    protected static RangeAnnotation IntervalJoinRangeMapAnnotation(AbstractFunctionCallExpression fexp) {
        Iterator<IExpressionAnnotation> annotationIter = fexp.getAnnotations().values().iterator();
        while (annotationIter.hasNext()) {
            IExpressionAnnotation annotation = annotationIter.next();
            if (annotation instanceof RangeAnnotation) {
                return (RangeAnnotation) annotation;
            }
        }
        return null;
    }

    protected static void setIntervalForwardScanJoinOp(AbstractBinaryJoinOperator op, FunctionIdentifier fi,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, IOptimizationContext context,
            IntervalPartitions intervalPartitions) {

        IIntervalMergeJoinCheckerFactory mjcf =
                getIntervalMergeJoinCheckerFactory(fi, intervalPartitions.getRangeMap());
        op.setPhysicalOperator(new IntervalForwardScanJoinPOperator(op.getJoinKind(),
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(), mjcf, intervalPartitions));
    }

    protected static IntervalPartitions getIntervalPartitions(AbstractBinaryJoinOperator op, FunctionIdentifier fi,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, RangeMap rangeMap,
            IOptimizationContext context) throws AlgebricksException {

        List<LogicalVariable> leftPartitionVar = new ArrayList<>(2);
        leftPartitionVar.add(context.newVar());
        leftPartitionVar.add(context.newVar());
        List<LogicalVariable> rightPartitionVar = new ArrayList<>(2);
        rightPartitionVar.add(context.newVar());
        rightPartitionVar.add(context.newVar());
        //        List<LogicalVariable> leftPartitionVar = new Arrays.asList(context.newVar(), context.newVar());
        //        List<LogicalVariable> rightPartitionVar = Arrays.asList(context.newVar(), context.newVar());

        insertPartitionSortKey(op, LEFT, leftPartitionVar, sideLeft.get(0), context);
        insertPartitionSortKey(op, RIGHT, rightPartitionVar, sideRight.get(0), context);

        List<IntervalColumn> leftIC = Arrays.asList(new IntervalColumn(leftPartitionVar.get(0), leftPartitionVar.get(1),
                OrderOperator.IOrder.OrderKind.ASC));
        List<IntervalColumn> rightIC = Arrays.asList(new IntervalColumn(rightPartitionVar.get(0),
                rightPartitionVar.get(1), OrderOperator.IOrder.OrderKind.ASC));

        //If statement for partitioning types
        PartitioningType leftPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        PartitioningType rightPartitioningType = PartitioningType.ORDERED_PARTITIONED;
        if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPED_BY)) {
            leftPartitioningType = PartitioningType.ORDERED_PARTITIONED;
            rightPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPS)) {
            //default
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPING)) {
            rightPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERS)) {
            //default
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERED_BY)) {
            leftPartitioningType = PartitioningType.ORDERED_PARTITIONED;
            rightPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_STARTS)) {
            leftPartitioningType = PartitioningType.ORDERED_PARTITIONED;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_STARTED_BY)) {
            //default
        } else if (fi.equals(BuiltinFunctions.INTERVAL_ENDS)) {
            leftPartitioningType = PartitioningType.ORDERED_PARTITIONED;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_ENDED_BY)) {
            //Default
        } else if (fi.equals(BuiltinFunctions.INTERVAL_MEETS)) {
            leftPartitioningType = PartitioningType.ORDERED_PARTITIONED;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_MET_BY)) {
            rightPartitioningType = PartitioningType.ORDERED_PARTITIONED;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_BEFORE)) {
            leftPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_FOLLOWING;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_AFTER)) {
            leftPartitioningType = PartitioningType.ORDERED_PARTITIONED;
            rightPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_FOLLOWING;
        }
        return new IntervalPartitions(rangeMap, leftIC, rightIC, leftPartitioningType, rightPartitioningType);
    }

    protected static FunctionIdentifier isIntervalJoinCondition(ILogicalExpression e,
            Collection<LogicalVariable> inLeftAll, Collection<LogicalVariable> inRightAll,
            Collection<LogicalVariable> outLeftFields, Collection<LogicalVariable> outRightFields) {
        FunctionIdentifier fiReturn;
        boolean switchArguments = false;
        if (e.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
            FunctionIdentifier fi = fexp.getFunctionIdentifier();
            if (isIntervalFunction(fi)) {
                fiReturn = fi;
            } else {
                return null;
            }
            ILogicalExpression opLeft = fexp.getArguments().get(LEFT).getValue();
            ILogicalExpression opRight = fexp.getArguments().get(RIGHT).getValue();
            if (opLeft.getExpressionTag() != LogicalExpressionTag.VARIABLE
                    || opRight.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return null;
            }
            LogicalVariable var1 = ((VariableReferenceExpression) opLeft).getVariableReference();
            if (inLeftAll.contains(var1) && !outLeftFields.contains(var1)) {
                outLeftFields.add(var1);
            } else if (inRightAll.contains(var1) && !outRightFields.contains(var1)) {
                outRightFields.add(var1);
                fiReturn = reverseIntervalExpression(fi);
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
        } else {
            return null;
        }
    }

    private static IIntervalMergeJoinCheckerFactory getIntervalMergeJoinCheckerFactory(FunctionIdentifier fi,
            RangeMap rangeMap) {
        //Not supported for... @Translateinterval expression rule
        IIntervalMergeJoinCheckerFactory mjcf = new OverlappingIntervalMergeJoinCheckerFactory(rangeMap);
        if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPED_BY)) {
            mjcf = new OverlappedByIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPS)) {
            mjcf = new OverlapsIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERS)) {
            mjcf = new CoversIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERED_BY)) {
            mjcf = new CoveredByIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_STARTS)) {
            mjcf = new StartsIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_STARTED_BY)) {
            mjcf = new StartedByIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_ENDS)) {
            mjcf = new EndsIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_ENDED_BY)) {
            mjcf = new EndedByIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_MEETS)) {
            mjcf = new MeetsIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_MET_BY)) {
            mjcf = new MetByIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_BEFORE)) {
            mjcf = new BeforeIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_AFTER)) {
            mjcf = new AfterIntervalMergeJoinCheckerFactory();
        }
        return mjcf;
    }

    private static boolean isIntervalFunction(FunctionIdentifier fi) {
        return INTERVAL_JOIN_CONDITIONS.containsKey(fi);
    }

    private static FunctionIdentifier reverseIntervalExpression(FunctionIdentifier fi) {
        if (INTERVAL_JOIN_CONDITIONS.containsKey(fi)) {
            return INTERVAL_JOIN_CONDITIONS.get(fi);
        }
        return null;
    }

    private static void insertPartitionSortKey(AbstractBinaryJoinOperator op, int branch,
            List<LogicalVariable> partitionVars, LogicalVariable intervalVar, IOptimizationContext context)
            throws AlgebricksException {
        MutableObject<ILogicalExpression> intervalExp =
                new MutableObject<>(new VariableReferenceExpression(intervalVar));

        List<Mutable<ILogicalExpression>> assignExps = new ArrayList<>();
        // Start partition
        IFunctionInfo startFi = FunctionUtil.getFunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_START);
        @SuppressWarnings("unchecked")
        ScalarFunctionCallExpression startPartitionExp = new ScalarFunctionCallExpression(startFi, intervalExp);
        assignExps.add(new MutableObject<ILogicalExpression>(startPartitionExp));
        // End partition
        IFunctionInfo endFi = FunctionUtil.getFunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_END);
        @SuppressWarnings("unchecked")
        ScalarFunctionCallExpression endPartitionExp = new ScalarFunctionCallExpression(endFi, intervalExp);
        assignExps.add(new MutableObject<ILogicalExpression>(endPartitionExp));

        AssignOperator ao = new AssignOperator(partitionVars, assignExps);
        ao.setExecutionMode(op.getExecutionMode());
        AssignPOperator apo = new AssignPOperator();
        ao.setPhysicalOperator(apo);
        Mutable<ILogicalOperator> aoRef = new MutableObject<>(ao);
        ao.getInputs().add(op.getInputs().get(branch));
        op.getInputs().set(branch, aoRef);

        context.computeAndSetTypeEnvironmentForOperator(ao);
    }
}
