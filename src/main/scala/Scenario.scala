import org.apache.spark.sql.SparkSession
import java.sql.Connection
import scala.io.StdIn.{readInt, readLine}

object Scenario {


    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    println("created spark session")

    def Scenario1(con: Connection): Unit = {

        val menuSelection = readInt()

        def displayJobs(con: Connection): Unit = {
            val data = readInt()
            //data.menu

            //val scanner = new Nothing(System.in)
            //val choice = scanner.nextInt

            //choice match {
                //case 1 =>
                    //Answer1a = (con)
                    //menu()

                //case 2 =>
                // Perform "encrypt number" case.

                //case 3 =>
                // Perform "decrypt number" case.

                //case 4 =>
                // Perform "quit" case.

                //case 5 =>

                //case 6 =>


            }
            println(
                """
                  |Main Menu
                  |1. Scenario 1
                  |2. Scenario 2
                  |3. Scenario 3
                  |4. Scenario 4
                  |5. Scenario 5
                  |6. Scenario 6
                  |7. Quit
                  |""".stripMargin)


        }



}