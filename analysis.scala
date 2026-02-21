import org.apache.spark.sql.SparkSession

val sparkSession = spark 
println("=== Chargement des données S&P 500 ===")

// Chargement du Dataset
val df = sparkSession.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/data/sp500/sp500_stock_price.csv")

// 1. Affichage du schema
println("Schema des donnees :")
df.printSchema()

// 2. Apercu des donnees
println("Apercu des 5 premieres lignes :")
df.show(5)

// 3. Statistiques descriptives (Min, Max, Moyenne, etc.)
println("Statistiques descriptives pour Open, Close et Volume :")
df.describe("open", "close", "volume").show()

// 4. Nombre total de lignes
println(s"Nombre total d'enregistrements : ${df.count()}")
