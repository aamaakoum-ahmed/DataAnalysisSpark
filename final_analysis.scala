import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

val sparkSession = spark
import sparkSession.implicits._

println("\n=== EXECUTION DE L'ANALYSE FINALE ===")

// 1. Chargement explicite (tout sur une ligne pour eviter le bug du Reader)
val dfStock = spark.read.option("header","true").option("inferSchema","true").csv("/data/sp500/sp500_stock_price.csv").withColumn("Year", year(col("Date")))

val dfShares = spark.read.option("header","true").option("inferSchema","true").csv("/data/sp500/sharesOutstanding.csv")

// 2. Conversion du type de sharesOutstanding (il est lu comme String, il faut un Double)
val dfSharesClean = dfShares.withColumn("sharesOutstanding", col("shareOutstanding").cast(DoubleType))

// 3. Jointure
val dfJoined = dfStock.join(dfSharesClean, "Symbol")

// 4. Calcul du Ratio et classement
val dfRatio = dfJoined.withColumn("TurnoverRatio", col("Volume") / col("sharesOutstanding"))

val windowSpec = Window.partitionBy("Year").orderBy(desc("TurnoverRatio"))

val result = dfRatio.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") === 1).select("Year", "Symbol", "TurnoverRatio").orderBy("Year")

println("\n[RESULTAT 1] Top rotation par annee :")
result.show(20)

// 5. Analyses groupees
println("\n[RESULTAT 2] Variation moyenne (High-Low) par annee :")
dfStock.withColumn("Diff", col("High") - col("Low")).groupBy("Year").agg(avg("Diff").as("MeanDiff")).orderBy(desc("MeanDiff")).show(5)

val volumeStats = dfStock.groupBy("Year").agg(sum("Volume").as("TotalVolume")).cache()

println("\n[RESULTAT 3] Volumes Max/Min :")
volumeStats.orderBy(desc("TotalVolume")).show(1)
volumeStats.orderBy(asc("TotalVolume")).show(1)

println("\n=== FIN DE L'ANALYSE ===")
