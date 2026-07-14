import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * APPROCCIO 2: aggregateByKey + combinations (senza self-join).
 *
 * Differenze rispetto alla V1:
 *  - nessun self-join: le coppie sono generate localmente per ogni giorno
 *    con .combinations(2), che restituisce un Iterator (streaming, non materializza
 *    tutte le coppie in memoria);
 *  - deduplica e raggruppamento avvengono in un unico shuffle (aggregateByKey su Set);
 *  - il conteggio usa reduceByKey(_ + _) con combiner map-side, invece di
 *    concatenare liste di date (reduceByKey(_ ++ _)), che è il vero divoratore di memoria;
 *  - il massimo si trova con reduce, senza sortBy (che è un sort globale con shuffle);
 *  - le date della coppia vincente si recuperano con un filtro sulle celle già in cache,
 *    senza rigenerare le coppie e senza ulteriori shuffle.
 *
 * L'output è identico a quello della V1 (stesso criterio di ordinamento della coppia),
 * così i due approcci sono confrontabili direttamente.
 */
object EarthquakeCoOccurrenceV2 {

  private def locKey(c: (Double, Double)): String = s"${c._1},${c._2}"

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: EarthquakeCoOccurrenceV2 <path_to_csv> [num_partitions]")
      System.exit(1)
    }

    val filename = args(0)
    val numPartitions: Option[Int] =
      if (args.length == 2) Some(args(1).toInt) else None

    val spark = SparkSession.builder
      .appName("Earthquake Application - V2 (combinations)")
      .getOrCreate()

    val baseData = spark.read.option("header", "true").csv(filename).rdd

    val data = numPartitions match {
      case Some(p) => baseData.repartition(p)
      case None    => baseData
    }

    // Parsing e normalizzazione (identici alla V1)
    val parsed = data.map { row =>
      val lat = BigDecimal(row.getAs[String]("latitude"))
        .setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
      val lon = BigDecimal(row.getAs[String]("longitude"))
        .setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
      val day = row.getAs[String]("date").substring(0, 10)
      (day, (lat, lon))
    }

    // Deduplica + raggruppamento in UN SOLO shuffle
    // Per ogni giorno ottengo l'insieme delle celle attive.
    // Il Set elimina i duplicati (stessa cella, stesso giorno) senza bisogno di distinct().
    val zero = Set.empty[(Double, Double)]
    val cellsByDay = numPartitions match {
      case Some(p) =>
        parsed.aggregateByKey(zero, p)((s, c) => s + c, (a, b) => a union b)
      case None =>
        parsed.aggregateByKey(zero)((s, c) => s + c, (a, b) => a union b)
    }

    // Questo RDD è piccolo (una riga per giorno) e viene riusato due volte: lo teniamo in cache.
    cellsByDay.persist(StorageLevel.MEMORY_AND_DISK)

    // Generazione delle coppie e conteggio
    // combinations(2) su una sequenza ordinata produce ogni coppia una sola volta,
    // già in ordine lessicografico: niente coppie simmetriche, niente self-pair,
    // quindi nessun filtro da applicare dopo.
    val pairCounts = {
      val pairs = cellsByDay.flatMap { case (day, cells) =>
        cells.toSeq.sortBy(locKey)
          .combinations(2)
          .map(p => ((p(0), p(1)), 1))
      }
      numPartitions match {
        case Some(p) => pairs.reduceByKey(_ + _, p)
        case None    => pairs.reduceByKey(_ + _)
      }
    }

    // Coppia col massimo numero di co-occorrenze (senza sort globale)
    val (topPair, topCount) = pairCounts.reduce { (a, b) =>
      if (a._2 >= b._2) a else b
    }
    val (locA, locB) = topPair

    // Date della sola coppia vincente
    // Non rigenero le coppie: basta controllare quali giorni contengono entrambe le celle.
    // Lookup O(1) sul Set, nessuno shuffle.
    val dates = cellsByDay
      .filter { case (_, cells) => cells.contains(locA) && cells.contains(locB) }
      .map(_._1)
      .collect()
      .sorted

    println(s"((${locA._1}, ${locA._2}), (${locB._1}, ${locB._2}))")
    dates.foreach(println)

    System.err.println(s"[V2] co-occorrenze = $topCount, date stampate = ${dates.length}")

    spark.stop()
  }
}