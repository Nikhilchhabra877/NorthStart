import com.methods.SparkContext

object connectLoyalty extends App with SparkContext{
    val spark = getSparkSession(appName = "practice-spark")
    import spark.implicits._

    /*
    MAUI/QA:
    url: test-aurora-postgresql-egloyalty-loyalty-qa.cluster-cq38trbgzcgk.us-west-2.rds.amazonaws.com
    dbname: loyalty
    username: loyalty_appuser_qa
    password: 5fbc7e95-c155-49ff-a866-33ff1716727c
    */

    val df = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://test-aurora-postgresql-egloyalty-loyalty-qa.cluster-cq38trbgzcgk.us-west-2.rds.amazonaws.com:5432/loyalty")
      .option("query", "select * from membership.enrollment_sources")
      .option("user", "loyalty_appuser_qa")
      .option("password", "5fbc7e95-c155-49ff-a866-33ff1716727c")
      .load()

    // Show sample records from data frame
    df.show(false)
}
