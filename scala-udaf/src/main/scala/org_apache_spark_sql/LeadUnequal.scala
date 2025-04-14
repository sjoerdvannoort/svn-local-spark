package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.trees.TreePattern.{TreePattern, AGGREGATE_EXPRESSION,UNRESOLVED_WINDOW_EXPRESSION, WINDOW_EXPRESSION}
import org.apache.spark.sql.errors.{DataTypeErrorsBase, QueryCompilationErrors, QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._


/**
 * A window function is a function that can only be evaluated in the context of a window operator.
 */
trait SvnWindowFunction extends Expression {
  /** Frame in which the window operator must be executed. */
  def frame: WindowFrame = UnspecifiedFrame
}

case class LeadUnequal(valueExpr: Expression, evalExpr: Expression) extends AggregateWindowFunction with BinaryLike[Expression] {

  override def prettyName: String = "lead_unequal"

  override def left: Expression = valueExpr
  override def right: Expression = evalExpr

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = valueExpr.dataType

  override val frame: WindowFrame = SpecifiedWindowFrame(RowFrame, CurrentRow, UnboundedFollowing)

  // The attributes used to keep equality evaluation and lead values
  private lazy val equalEvaluation =
    AttributeReference("equalEvaluation", evalExpr.dataType)()
  private lazy val valueWithEqualEvaluation =
    AttributeReference("valueWithEqualEvaluation", valueExpr.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    valueWithEqualEvaluation :: equalEvaluation :: Nil

  private lazy val nullValue = Literal.create(null, valueExpr.dataType)
  private lazy val nullEval = Literal.create(null, evalExpr.dataType)

  override lazy val initialValues: Seq[Literal] = Seq(
    /* valueWithEqualEvaluation = */ nullValue,
    /* equalEvaluation = */ nullEval
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
      If(valueWithEqualEvaluation.isNull && !equalEvaluation.isNull
          ,If(EqualTo(equalEvaluation,evalExpr),valueWithEqualEvaluation,valueExpr),valueWithEqualEvaluation)
      , If(equalEvaluation.isNull,evalExpr,equalEvaluation) 
  )
    
  override lazy val mergeExpressions = throw QueryExecutionErrors.mergeUnsupportedByWindowFunctionError(prettyName)

  override lazy val evaluateExpression: AttributeReference = valueWithEqualEvaluation

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): LeadUnequal =
    copy(valueExpr = newLeft, evalExpr = newRight)
}