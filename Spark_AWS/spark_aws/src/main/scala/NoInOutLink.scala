import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object NoInOutLink {
    def main(args: Array[String]) {
        val MASTER_IP ="54-172-106-255"
        val MASTER_ADDRESS = "ec2-" + MASTER_IP + ".compute-1.amazonaws.com" 
        val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
        val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
        val INPUT_DIR = HDFS_MASTER + "/sample_input"
        val OUTPUT_DIR = HDFS_MASTER + "/output"
        val links_file = INPUT_DIR + "/links-simple-sorted.txt"
        val titles_file = INPUT_DIR + "/titles-sorted.txt"
        
        val num_partitions = 10

        val conf = new SparkConf()
            .setMaster(SPARK_MASTER) 
            .setAppName("NoInOutLink")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions)

        val outlinks = links
            .map(line => line.split(":")(0))
            .filter(key => key != "")
            .distinct()
            .cache()

        val inlinks = links
            .map(line => line.split(":")(1))
            .flatMap(line => line.split(" "))
            .filter(key => key != "")
            .distinct()
            .cache()

        val titles = sc
            .textFile(titles_file, num_partitions)
            .zipWithIndex()
            .map{ case (line, i) => (i+1, line)}
            .cache()

        /* No Outlinks */
        val outlinks_pair = outlinks.map(key => (key.toLong, 0))
        val no_outlinks = titles.subtractByKey(outlinks_pair)
        println("[ NO OUTLINKS ]")
        no_outlinks.sortByKey().take(10).foreach(println)

        /* No Inlinks */
        val inlinks_pair = inlinks.map(key => (key.toLong, 0))
        val no_inlinks = titles.subtractByKey(inlinks_pair)
        println("[ NO INLINKS ]")
        no_inlinks.sortByKey().take(10).foreach(println)

        sc.stop()
    }
}

/* https://data-flair.training/blogs/spark-rdd-operations-transformations-actions/ */
/* https://data-flair.training/blogs/apache-spark-map-vs-flatmap/ */
/* https://stackoverflow.com/questions/31212962/how-to-add-line-number-into-each-line */
