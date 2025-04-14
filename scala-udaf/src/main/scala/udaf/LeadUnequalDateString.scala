import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions
import java.sql.Date
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

package spark.udaf
{
    case class LeadUnequalDateStringInput(var col:Date, var ord:String)
    case class LeadUnequalDateStringBuffer(var source: String, var result: Date)

    object LeadUnequalDateString extends Aggregator[LeadUnequalDateStringInput, LeadUnequalDateStringBuffer, Date] {
        def zero: LeadUnequalDateStringBuffer = LeadUnequalDateStringBuffer(null, null)

        def reduce(buffer: LeadUnequalDateStringBuffer, data: LeadUnequalDateStringInput): LeadUnequalDateStringBuffer = {
            if (buffer.source == null){
                // first row in window will store the source reference value
                buffer.source = data.ord
            }
            else {
                // after the first row, check if result isn't set yet
                if (buffer.result == null) {
                    if (data.ord != buffer.source){
                        // if the current reference is unequal to the source reference, get the current value
                        buffer.result = data.col
                    }
                }
            }
            // return the buffer
            buffer
        }

         def merge(b1: LeadUnequalDateStringBuffer, b2: LeadUnequalDateStringBuffer): LeadUnequalDateStringBuffer = {
            //this should not occur as we would always have an order by
            b1
         }

         def finish(reduction: LeadUnequalDateStringBuffer): Date = {
            return reduction.result
         }
         // The Encoder for the intermediate value type
        val bufferEncoder: Encoder[LeadUnequalDateStringBuffer] = Encoders.product
        // The Encoder for the final output value type
        val outputEncoder: Encoder[Date] = ExpressionEncoder[Date]()

        // Helper function to be able to register this in pyspark
        def register(spark: SparkSession): Unit = {
            spark.udf.register("LEAD_UNEQUAL_DATE_STRING", functions.udaf(LeadUnequalDateString))
        }
    }
}
