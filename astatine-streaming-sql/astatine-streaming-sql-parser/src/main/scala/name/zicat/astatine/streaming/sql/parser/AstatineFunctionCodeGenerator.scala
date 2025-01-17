/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package name.zicat.astatine.streaming.sql.parser

import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.table.planner.codegen.CodeGenUtils.{boxedTypeTermForType, newName}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.{CodeGenUtils, CodeGeneratorContext}
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.table.runtime.generated.GeneratedOperator
import org.apache.flink.table.types.logical.LogicalType

/**
 * AstatineFunctionCodeGenerator.
 */
object AstatineFunctionCodeGenerator extends Logging{

  private val ELEMENT = "rowData"

  def generateFilterFunction[IN <: Any, OUT <: Any](
      ctx: CodeGeneratorContext,
      name: String,
      processCode: String,
      inputType: LogicalType,
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM): GeneratedOperator[OneInputStreamOperator[IN, OUT]] = {
    val operatorName = newName(name)
    val inputTypeTerm = boxedTypeTermForType(inputType)

    val operatorCode =
      j"""
      public class $operatorName implements org.apache.flink.api.common.functions.FilterFunction {

        private final java.lang.Object[] references;
        ${ctx.reuseMemberCode()}

        public $operatorName(java.lang.Object[] references) {
          this.references = references;
          ${ctx.reuseInitCode()}
        }

        @Override
        public boolean filter(java.lang.Object $ELEMENT) throws Exception {
          $inputTypeTerm $inputTerm = ($inputTypeTerm) $ELEMENT;
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseLocalVariableCode()}
          ${ctx.reuseInputUnboxingCode()}
          $processCode
        }
      }
    """.stripMargin

    LOG.debug(s"Compiling OneInputStreamOperator Code:\n$name")
    LOG.trace(s"Code: \n$operatorCode")
    new GeneratedOperator(operatorName, operatorCode, ctx.references.toArray, ctx.tableConfig)
  }

  def generateMapFunction[IN <: Any, OUT <: Any](
     ctx: CodeGeneratorContext,
     name: String,
     processCode: String,
     inputType: LogicalType,
     inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM): GeneratedOperator[OneInputStreamOperator[IN, OUT]] = {
    val operatorName = newName(name)
    val inputTypeTerm = boxedTypeTermForType(inputType)

    val operatorCode =
      j"""
      public class $operatorName implements org.apache.flink.api.common.functions.MapFunction {

        private final java.lang.Object[] references;
        ${ctx.reuseMemberCode()}

        public $operatorName(java.lang.Object[] references) {
          this.references = references;
          ${ctx.reuseInitCode()}
        }

        @Override
        public java.lang.Object map(java.lang.Object $ELEMENT) throws Exception {
          $inputTypeTerm $inputTerm = ($inputTypeTerm) $ELEMENT;
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseLocalVariableCode()}
          ${ctx.reuseInputUnboxingCode()}
          $processCode
          return out;
        }
      }
    """.stripMargin

    LOG.debug(s"Compiling OneInputStreamOperator Code:\n$name")
    LOG.trace(s"Code: \n$operatorCode")
    new GeneratedOperator(operatorName, operatorCode, ctx.references.toArray, ctx.tableConfig)
  }
}
