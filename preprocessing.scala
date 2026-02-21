import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


val sparkSession = spark

println("\n=== ETAPE 2 : PRETRAITEMENT ET SCHEMA MANUEL ===")

// 1. Definition de schema
val sp500Schema = StructType(Array(
  StructField("Date", DateType, true),
  StructField("Symbol", StringType, true),
  StructField("Open", DoubleType, true),
  StructField("High", DoubleType, true),
  StructField("Low", DoubleType, true),
  StructField("Close", DoubleType, true),
  StructField("Adj Close", DoubleType, true),
  StructField("Volume", LongType, true)
))

// 2. Chargement avec le schema impose (plus rapide et plus precis)
val dfRaw = sparkSession.read.option("header", "true").option("dateFormat", "yyyy-MM-dd").schema(sp500Schema).csv("/data/sp500/sp500_stock_price.csv")

// 3. Suppression de la colonne inutile (Adj Close)
val dfFinal = dfRaw.drop("Adj Close")

// 4. Verification du resultat
println("\nNouveau Schéma (sans Adj Close) :")
dfFinal.printSchema()

println("\nApercu des donnees propres :")
dfFinal.show(5)

// 5. Petite analyse de verification : Nombre de symboles uniques
val uniqueSymbols = dfFinal.select("Symbol").distinct().count()
println(s"\nNombre d'entreprises différentes dans le dataset : $uniqueSymbols")
