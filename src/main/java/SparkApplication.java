

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SparkApplication {

  public static void main(String[] args){
    String logFile="/usr/local/Cellar/apache-spark/2.4.3/README.md";
    SparkSession spark= SparkSession.builder().appName("Simple App").getOrCreate();
    Dataset<String> logData=spark.read().textFile(logFile).cache();

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
    int n = 100000 * slices;
    List<Integer> l = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      l.add(i);
    }

     //long numAs= logData.filter(s->s.contains("a")).count();
   // long numBs= logData.filter(s->s.contains("b")).count();

    //System.out.println("Lines with a: "+numAs+ ", lines with b: "+ numBs );
    long numAs= logData.count();
    System.out.println(("Lines count: "+ numAs));
    spark.stop();

  }
}


