import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.expressions._

case class MultiplyOptimizationRule(spark: SparkSession) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Multiply(left, right, failOnError)
      if right.isInstanceOf[Literal] && right.asInstanceOf[Literal].value.asInstanceOf[Int] == 1
    => left
    case Multiply(left, right, failOnError)
      if left.isInstanceOf[Literal] && left.asInstanceOf[Literal].value.asInstanceOf[Int] == 1
    => right
  }

}

class MultiplyOptimization extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session =>
      MultiplyOptimizationRule(session)
    }
  }
}

// 通过spark.sql.extensions 提交，将jar包放入spark的classPath
// bin/spark-sql --jars bigdata-camp.jar --conf spark.sql.extensions=MultiplyOptimizationRule