/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.flink.table.planner.plan.utils.RelExplainUtil
import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.calcite.util.Litmus
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Relational expression that apply a number of projects to every input row,
  * hence we will get multiple output rows for an input row.
  *
  * <p/> Values of expand_id should be unique.
  *
  * @param cluster       cluster that this relational expression belongs to
  * @param traits        the traits of this rel
  * @param input         input relational expression
  * @param outputFieldNames the output field names
  * @param projects      all projects, each project contains list of expressions for
  *                      the output columns
  * @param expandIdIndex expand_id('$e') field index
  */
abstract class Expand(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode,
    val outputFieldNames: util.List[String],
    val projects: util.List[util.List[RexNode]],
    val expandIdIndex: Int)
  extends SingleRel(cluster, traits, input) {

  isValid(Litmus.THROW, null)

  override def isValid(litmus: Litmus, context: RelNode.Context): Boolean = {
    if (projects.size() <= 1) {
      return litmus.fail("Expand should output more than one rows, otherwise use Project.")
    }
    if (null == outputFieldNames) {
      return litmus.fail("Expand should have none empty output field names.")
    }
    if (projects.exists(_.size != outputFieldNames.size())) {
      return litmus.fail("project filed count is not equal to output field count.")
    }
    if (expandIdIndex < 0 || expandIdIndex >= outputFieldNames.size()) {
      return litmus.fail(
        "expand_id field index should be greater than 0 and less than output field count.")
    }
    val expandIdValues = new util.HashSet[Any]()
    for (project <- projects) {
      project.get(expandIdIndex) match {
        case literal: RexLiteral => expandIdValues.add(literal.getValue)
        case _ => return litmus.fail("expand_id value should not be null.")
      }
    }
    if (expandIdValues.size() != projects.size()) {
      return litmus.fail("values of expand_id should be unique.")
    }
    litmus.succeed()
  }

  override def deriveRowType(): RelDataType = {
    val fieldNullableMap = mutable.Map[Int, Boolean]()
    projects.map {
      project =>
        project.zipWithIndex.map {
          f => {
            val nullable = fieldNullableMap.get(f._2).getOrElse(f._1.getType.isNullable)
            fieldNullableMap.put(f._2, nullable || f._1.getType.isNullable)
          }
        }
    }
    val typeList = ListBuffer[RelDataType]()
    val typeFactory = cluster.getTypeFactory
    projects.get(0).zipWithIndex.map {
      f => {
        typeList
          .add(typeFactory.createTypeWithNullability(f._1.getType, fieldNullableMap.get(f._2).get))
      }
    }
    typeFactory.createStructType(typeList, outputFieldNames)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("projects", RelExplainUtil.projectsToString(projects, input.getRowType, getRowType))
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this.getInput) * projects.size()
    planner.getCostFactory.makeCost(rowCnt, rowCnt, 0)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    val childRowCnt = mq.getRowCount(this.getInput)
    if (childRowCnt != null) {
      childRowCnt * projects.size()
    } else {
      null.asInstanceOf[Double]
    }
  }
}
