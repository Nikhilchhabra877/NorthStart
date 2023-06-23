
import com.methods.SparkContext
import org.apache.log4j.{Level, Logger}

import java.sql.{Connection, DriverManager, Statement}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions.col

class connectLoyaltyTest extends AnyFunSuite with BeforeAndAfterAll with SparkContext{
  var namevarpostgres = ""
  var nameinmem = ""

  var connection: Connection = _
  val spark = getSparkSession(appName = "practice-spark-scala")

  override def beforeAll(): Unit = {

    // Create an in-memory H2 database and establish a connection
    connection = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")

    // Initialize the database schema
    val createTableSql =
      """
        |create table enrollment_sources(key int,code varchar(50),name varchar(50),description varchar(50));
        |insert into enrollment_sources values (1,'traditional_browser','traditional_browser','Traditional Browser');
        |insert into enrollment_sources values (2,'mobile_site','mobile_site','Mobile site');
        |insert into enrollment_sources values (3,'mobile_app','mobile_app','Mobile App');
        |insert into enrollment_sources values (4,'agent_tool','agent_tool','Agent Tool');
        |insert into enrollment_sources values (5,'offsite_enrollment','offsite_enrollment','Offsite Enrollment');""".stripMargin
    val statement = connection.createStatement()
    statement.execute(createTableSql)
    statement.close()
  }

  override def afterAll(): Unit = {
    // Close the database connection
    connection.close()
  }

  test("comparing H2 database and postgres") {
    try {
      val dfH2 = spark.read
        .format("jdbc")
        .option("url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
        .option("query", "select * from enrollment_sources")
        .load()

      nameinmem = dfH2.filter(col("key") === 1).select("name").toString()

      val df = spark.read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", "jdbc:postgresql://test-aurora-postgresql-egloyalty-loyalty-qa.cluster-cq38trbgzcgk.us-west-2.rds.amazonaws.com:5432/loyalty")
        .option("query", "select * from membership.enrollment_sources")
        .option("user", "loyalty_appuser_qa")
        .option("password", "5fbc7e95-c155-49ff-a866-33ff1716727c")
        .load()

      namevarpostgres = df.filter(col("key") === 1).select("name").toString()
    }
    catch {
      case e: Exception => println(e.getMessage)
    }
    assert(nameinmem == namevarpostgres, "Data match done")
    print("Data compare done")
  }
}
