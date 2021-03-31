import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        val iters = 10
        val d = 0.85

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions)
            .flatMap{ line =>
                val key = line.split(": ")(0)
                val value = line.split(": ")(1).split(" ").toList
                value.map( x => (key, x) )
            }
            .distinct()
            .groupByKey()
            .cache()

        // println("links")
        // links.foreach(println)

        val titles = sc
            .textFile(titles_file, num_partitions)
            .zipWithIndex()
            .map{ case (line, i) => ((i+1).toString, line) }
            .cache()

        /* PageRank */
        // initial PageRank
        val N = links.count()
        var ranks = links.mapValues(value => 100.0 / N)
        var ranks_has_inlink = ranks
        var ranks_no_inlink = ranks

        // println("initial pagerank for every page")
        // ranks.foreach(println)

        for (i <- 1 to iters) {
            val page_ranks = links
                .join(ranks)
                .values
                .flatMap{ case (pages, rank) => 
                    val num_outlinks = pages.filter(line => line != " ").size
                    pages.map( page => (page.toString, (rank / num_outlinks).toDouble) )
                }
            
            // println("pageranks of the neighbors")
            // page_ranks.foreach(println)

            ranks_has_inlink = page_ranks
                .reduceByKey(_+_)
                .mapValues( (1-d) * 100.0 / N + d * _ )

            ranks_no_inlink = ranks
                .subtractByKey(ranks_has_inlink)
                .mapValues( value => (1-d) * 100.0 / N )

            ranks = ranks_has_inlink.union(ranks_no_inlink)

            // println("pagerank at " + i + " th iteration")
            // ranks.foreach(println)
        }

        // normalization
        val sum = ranks.reduceByKey(_+_).values.sum()
        ranks = ranks.mapValues(100.0 / sum * _)

        println("[ PageRanks ]")
        val PageRanks = titles
            .join(ranks)
            .map{ case(key, (title, pr)) => (key, title, pr) }
            .sortBy(x => x._3, false)
            // .sortBy({case (key, title, pr) => pr}, false)
        PageRanks.take(10).foreach(println)

        sc.stop()
    }
}

// REFERENCES
/* https://github.com/abbas-taher/pagerank-example-spark2.0-deep-dive */
/* https://en.wikipedia.org/wiki/PageRank#Computation */
/* https://stackoverflow.com/questions/36696326/map-vs-mapvalues-in-spark/36696468 */
/* https://stackoverflow.com/questions/38705596/how-to-sum-values-of-column-within-rdd */
/* https://stackoverflow.com/questions/40213304/how-to-ascending-sort-a-multiple-array-of-spark-rdd-by-any-column-in-scala?rq=1 */
/* https://stackoverflow.com/questions/29820501/splitting-strings-in-apache-spark-using-scala */