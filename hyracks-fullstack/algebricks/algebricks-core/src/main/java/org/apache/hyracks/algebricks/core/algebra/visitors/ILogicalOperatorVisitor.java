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
package org.apache.hyracks.algebricks.core.algebra.visitors;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.*;

public interface ILogicalOperatorVisitor<R, T> {

    public R visitAggregateOperator(AggregateOperator op, T arg) throws AlgebricksException;

    public R visitRunningAggregateOperator(RunningAggregateOperator op, T arg) throws AlgebricksException;

    public R visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, T arg) throws AlgebricksException;

    public R visitGroupByOperator(GroupByOperator op, T arg) throws AlgebricksException;

    public R visitLimitOperator(LimitOperator op, T arg) throws AlgebricksException;

    public R visitInnerJoinOperator(InnerJoinOperator op, T arg) throws AlgebricksException;

    public R visitLeftOuterJoinOperator(LeftOuterJoinOperator op, T arg) throws AlgebricksException;

    public R visitNestedTupleSourceOperator(NestedTupleSourceOperator op, T arg) throws AlgebricksException;

    public R visitOrderOperator(OrderOperator op, T arg) throws AlgebricksException;

    public R visitAssignOperator(AssignOperator op, T arg) throws AlgebricksException;

    public R visitSelectOperator(SelectOperator op, T arg) throws AlgebricksException;

    public R visitDelegateOperator(DelegateOperator op, T arg) throws AlgebricksException;

    public R visitProjectOperator(ProjectOperator op, T arg) throws AlgebricksException;

    public R visitReplicateOperator(ReplicateOperator op, T arg) throws AlgebricksException;

    public R visitSplitOperator(SplitOperator op, T arg) throws AlgebricksException;

    public R visitMaterializeOperator(MaterializeOperator op, T arg) throws AlgebricksException;

    public R visitScriptOperator(ScriptOperator op, T arg) throws AlgebricksException;

    public R visitSubplanOperator(SubplanOperator op, T arg) throws AlgebricksException;

    public R visitSinkOperator(SinkOperator op, T arg) throws AlgebricksException;

    public R visitUnionOperator(UnionAllOperator op, T arg) throws AlgebricksException;

    public R visitIntersectOperator(IntersectOperator op, T arg) throws AlgebricksException;

    public R visitUnnestOperator(UnnestOperator op, T arg) throws AlgebricksException;

    public R visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, T arg) throws AlgebricksException;

    public R visitUnnestMapOperator(UnnestMapOperator op, T arg) throws AlgebricksException;

    public R visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, T arg) throws AlgebricksException;

    public R visitDataScanOperator(DataSourceScanOperator op, T arg) throws AlgebricksException;

    public R visitDistinctOperator(DistinctOperator op, T arg) throws AlgebricksException;

    public R visitExchangeOperator(ExchangeOperator op, T arg) throws AlgebricksException;

    public R visitWriteOperator(WriteOperator op, T arg) throws AlgebricksException;

    public R visitDistributeResultOperator(DistributeResultOperator op, T arg) throws AlgebricksException;

    public R visitWriteResultOperator(WriteResultOperator op, T arg) throws AlgebricksException;

    public R visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, T arg) throws AlgebricksException;

    public R visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, T arg) throws AlgebricksException;

    public R visitTokenizeOperator(TokenizeOperator op, T arg) throws AlgebricksException;

    public R visitForwardOperator(ForwardOperator op, T arg) throws AlgebricksException;

    public R visitWindowOperator(WindowOperator op, T arg) throws AlgebricksException;
}
