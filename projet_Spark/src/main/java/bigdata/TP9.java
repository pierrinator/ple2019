package bigdata;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;

public class TP9 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("TP Spark");
        JavaSparkContext context = new JavaSparkContext(conf);

        String path = args[0];
        JavaRDD<String> lines = context.textFile(path);


        System.out.println(lines.getNumPartitions());
        lines = lines.repartition(context.sc().getExecutorStorageStatus().length - 1);
        System.out.println(lines.getNumPartitions());

        JavaPairRDD<String, Long> pairedLines = lines.mapToPair(s -> {
            long pop = -1;
            String city = "";
            String[] tokens = s.split(",");
            if(tokens.length > 4 &&
                StringUtils.isNotEmpty(tokens[4]) &&
                StringUtils.isNumericSpace(tokens[4])) {
                    pop = Long.parseLong(tokens[4]);
                    city = tokens[2];
            }

            return new Tuple2<>(city, pop);
        });

        pairedLines = pairedLines.filter(stringLongTuple2 -> {
            return stringLongTuple2._2 != -1;
        });

        /* Exo 3 */
        JavaDoubleRDD doubleRdd = pairedLines.mapToDouble(stringLongTuple2 -> stringLongTuple2._2);
        StatCounter popStats = doubleRdd.stats();
        System.out.println(popStats);

        /* Exo 4 */
        double[] classEqiuvalence = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000};

        long[] histograms = doubleRdd.histogram(classEqiuvalence);
        for(double ec : classEqiuvalence) {
            System.out.println(doubleRdd.filter(pop -> pop > ec && pop < ec*10 -1).stats());
        }

        pairedLines.saveAsTextFile(args[1]);
        context.close();
    }
}
