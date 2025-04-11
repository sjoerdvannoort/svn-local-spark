package spark.udaf

import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.expressions.aggregate._


object SvnFunctionRegistration extends NativeFunctionRegistration {

  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    expression[LeadUnequal]("lead_unequal")
  )
}