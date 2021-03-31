import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {
    def main(args: Array[String]) {
      //AWS configurations - must be setup for every new cluster startup
        val MASTER_IP ="3-233-237-4"
        val MASTER_ADDRESS = "ec2-" + MASTER_IP + ".compute-1.amazonaws.com" 
        val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
        val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
        val INPUT_DIR = HDFS_MASTER + "/sample_input"
        //File directories
        val links_file = INPUT_DIR + "/links-simple-sorted.txt"
        val titles_file = INPUT_DIR + "/titles-sorted.txt"
 
        val num_partitions = 10
        val iters = 10
        val d = 0.85

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster(SPARK_MASTER)
            //.set("spark.driver.memory", "1g")
            //.set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions)
            .map{ line =>
                val key = line.split(": ")(0)
                val value = line.split(": ")(1).split(" ")
                (key, value)
            }

        val titles = sc
            .textFile(titles_file, num_partitions)
            .zipWithIndex()
            .map{ case (line, i) => ((i+1).toString, line) }

        val inlinks = links
            .values
            .flatMap(x => x)
            .filter(key => key != "")
            .distinct()
            .map(key => (key.toString, 0))


        /* PageRank */
        // initial PageRank
        val N = links.count()
        var ranks = links.mapValues(value => 100.0 / N)
        var ranks_has_inlink = ranks
        val ranks_no_inlink = titles.subtractByKey(inlinks).mapValues(value => (1-d) * 100.0 / N)

        for (i <- 1 to iters) {
            val page_ranks = links
                .join(ranks)
                .values
                .flatMap{ case (pages, rank) => 
                    val num_outlinks = pages.size
                    pages.map( page => (page.toString, (rank / num_outlinks).toDouble) )
                }

            ranks_has_inlink = page_ranks
                .reduceByKey(_+_)
                .mapValues( (1-d) * 100.0 / N + d * _ )

            ranks = ranks_has_inlink.union(ranks_no_inlink)

        }

        // normalization
        val sum = ranks.values.sum()
        ranks = ranks.mapValues(100.0 / sum * _)

        println("[ PageRanks ]")
        val PageRanks = titles
            .join(ranks)
            .map{ case(key, (title, pr)) => (key, title, pr) }
            .sortBy(x => x._3, false)
            .take(10).foreach(println)
    }
}

// REFERENCES
/* https://github.com/abbas-taher/pagerank-example-spark2.0-deep-dive */
/* https://en.wikipedia.org/wiki/PageRank#Computation */
/* https://stackoverflow.com/questions/36696326/map-vs-mapvalues-in-spark/36696468 */
/* https://stackoverflow.com/questions/38705596/how-to-sum-values-of-column-within-rdd */
/* https://stackoverflow.com/questions/40213304/how-to-ascending-sort-a-multiple-array-of-spark-rdd-by-any-column-in-scala?rq=1 */
/* https://stackoverflow.com/questions/29820501/splitting-strings-in-apache-spark-using-scala */
