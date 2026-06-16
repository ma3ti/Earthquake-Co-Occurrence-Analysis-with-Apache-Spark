import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object EarthquakeCoOccurrence {
  def main(args: Array[String]): Unit = {
    
    // Controllo che il filename sia passato come argomento (minimo 1, massimo 2)
    if (args.length < 1) {
      System.err.println("Usage: EarthquakeCoOccurrence <path_to_csv> [num_partitions]")
      System.exit(1)
    }
    
    val filename = args(0)

    val spark = SparkSession.builder
      .appName("Earthquake Application")
      .getOrCreate()
      
    val sc = spark.sparkContext

    // Lettura dei dati base
    val baseData = spark.read.option("header", "true").csv(filename).rdd

    // Logica Partizioni: se viene passato un numero, forzo il repartition. 
    // Altrimenti lascio decidere a Spark (comportamento di default)
    val data = if (args.length == 2) {
      val numPartitions = args(1).toInt
      baseData.repartition(numPartitions)
    } else {
      baseData
    }

    // Parsing, arrotondamento e rimozione duplicati (stessa cella, stesso giorno)
    val parsedData = data.map(row => {
      val latStr = row.getAs[String]("latitude")
      val lonStr = row.getAs[String]("longitude")
      val dateFull = row.getAs[String]("date")
      
      val lat = latStr.toDouble
      val lon = lonStr.toDouble
      
      // Finestra temporale di 1 giorno
      val dateDay = dateFull.substring(0, 10) 
      
      // Arrotondamento alla prima cifra decimale
      val roundedLat = BigDecimal(lat).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
      val roundedLon = BigDecimal(lon).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
      
      (dateDay, (roundedLat, roundedLon))
    }).distinct() 

    // Trovo le co-occorrenze (Self-Join sulle date)
    val coOccurrences = parsedData.join(parsedData)
      .filter { case (date, (loc1, loc2)) =>
        // Assicuro un ordine lessicografico per evitare doppioni (A,B) e (B,A) e scartare (A,A)
        val loc1Str = s"${loc1._1},${loc1._2}"
        val loc2Str = s"${loc2._1},${loc2._2}"
        loc1Str < loc2Str
      }

    // Mappatura e Aggregazione per Locazione
    val pairDates = coOccurrences.map { case (date, (loc1, loc2)) =>
      ((loc1, loc2), List(date))
    }.reduceByKey(_ ++ _) // Unisco le liste di date per la stessa coppia

    // Trovo la coppia con il numero massimo di co-occorrenze
    val topPair = pairDates.sortBy(_._2.size, ascending = false).first()

    // Formattazione dell'output
    val locations = topPair._1
    val datesSorted = topPair._2.sorted

    println(s"((${locations._1._1}, ${locations._1._2}), (${locations._2._1}, ${locations._2._2}))")
    datesSorted.foreach(println) 

    spark.stop()
  }
}