

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;

public class SparkApplication {

  public static class Record implements Serializable{
    private int key;
    private String value;

    public int getKey() {
      return key;
    }

    public void setKey(int key) {
      this.key = key;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }

  public static void main(String[] args){
    SparkSession spark= SparkSession.builder().appName("Simple App with Hive").getOrCreate();

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    spark.stop();

  }

  public  void processPI(String[] args){
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

    JavaRDD<Integer> dataSet=jsc.parallelize(l,slices);
    LocalDateTime startTime=LocalDateTime.now();
    int count = dataSet.map(new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer integer) {
        double x = Math.random() * 2 - 1;
        double y = Math.random() * 2 - 1;
        return (x * x + y * y < 1) ? 1 : 0;
      }
    }).reduce(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer integer, Integer integer2) {
        return integer + integer2;
      }
    });

    LocalDateTime endTime= LocalDateTime.now();
    Period period=new Period(startTime,endTime);
    System.out.println("Pi is roughly " + 4.0 * count / n +" Time taken: "+ period.getMillis()+ " Slice no: "+ slices);


    jsc.stop();


    //long numAs= logData.filter(s->s.contains("a")).count();
    // long numBs= logData.filter(s->s.contains("b")).count();

    //System.out.println("Lines with a: "+numAs+ ", lines with b: "+ numBs );
    //long numAs= logData.count();
    //System.out.println(("Lines count: "+ numAs));
    spark.stop();
  }
}


