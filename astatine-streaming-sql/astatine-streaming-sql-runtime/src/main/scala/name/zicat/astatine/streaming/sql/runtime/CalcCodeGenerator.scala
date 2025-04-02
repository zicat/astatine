package name.zicat.astatine.streaming.sql.runtime

import org.apache.calcite.rex.{RexCall, RexNode, RexVisitorImpl}
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.data.{BoxedWrapperRowData, RowData}
import org.apache.flink.table.functions.FunctionKind
import org.apache.flink.table.planner.codegen.{CodeGenUtils, CodeGeneratorContext, ExprCodeGenerator}
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.types.logical.RowType


/**
 * CalcCodeGenerator.
 */
object CalcCodeGenerator {

  def generateFilterFunction(
     ctx: CodeGeneratorContext,
     rowType: RowType,
     condition : RexNode,
     opName: String): CodeGenOperatorFactory[RowData] = {
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val processCode = generateFilterCode(
      ctx,
      rowType,
      condition,
      eagerInputUnboxingCode = true)
    val genOperator =
      FunctionCodeGenerator.generateFilterFunction[RowData, RowData](
        ctx,
        opName,
        processCode,
        rowType,
        inputTerm = inputTerm)
    new CodeGenOperatorFactory(genOperator)
  }

  def generateMapFunction(
      ctx: CodeGeneratorContext,
      inputType: RowType,
      outputType: RowType,
      projection: Seq[RexNode],
      opName: String): CodeGenOperatorFactory[RowData] = {
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val processCode = generateMapCode(
      ctx,
      inputType,
      outputType,
      classOf[BoxedWrapperRowData],
      projection,
      eagerInputUnboxingCode = true)
    val genOperator =
      FunctionCodeGenerator.generateMapFunction[RowData, RowData](
        ctx,
        opName,
        processCode,
        inputType,
        inputTerm = inputTerm)
    new CodeGenOperatorFactory(genOperator)
  }

  private def generateFilterCode(
    ctx: CodeGeneratorContext,
    rowType: RowType,
    condition: RexNode,
    inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
    eagerInputUnboxingCode: Boolean): String = {
    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(rowType, inputTerm = inputTerm)
    val filterCondition = exprGenerator.generateExpression(condition)
    s"""
              |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
              |${filterCondition.code}
              |return ${filterCondition.resultTerm};
              |""".stripMargin
  }

  private def generateMapCode(
     ctx: CodeGeneratorContext,
     inputType: RowType,
     outRowType: RowType,
     outRowClass: Class[_ <: RowData],
     projection: Seq[RexNode],
     inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
     eagerInputUnboxingCode: Boolean,
     retainHeader: Boolean = false): String = {

    projection.foreach(_.accept(ScalarFunctionsValidator))

    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(inputType, inputTerm = inputTerm)

    def produceProjectionCode: String = {
      val projectionExprs = projection.map(exprGenerator.generateExpression)
      val projectionExpression =
        exprGenerator.generateResultExpression(projectionExprs, outRowType, outRowClass)

      val projectionExpressionCode = projectionExpression.code

      val header = if (retainHeader) {
        s"${projectionExpression.resultTerm}.setRowKind($inputTerm.getRowKind());"
      } else {
        ""
      }
      s"""
         |$header
         |$projectionExpressionCode
         |""".stripMargin
    }
    s"""
       |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
       |$produceProjectionCode
       |""".stripMargin

  }

  private object ScalarFunctionsValidator extends RexVisitorImpl[Unit](true) {
    override def visitCall(call: RexCall): Unit = {
      super.visitCall(call)
      call.getOperator match {
        case bsf: BridgingSqlFunction if bsf.getDefinition.getKind != FunctionKind.SCALAR =>
          throw new ValidationException(
            s"Invalid use of function '$bsf'. " +
              s"Currently, only scalar functions can be used in a projection or filter operation.")
        case _ => // ok
      }
    }
  }
}
