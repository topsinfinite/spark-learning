import static org.apache.spark.sql.functions.col;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;

public class SparkApplication {

  public static void main(String[] args) {
    String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
    String filePath = "/Users/tfatayo/cdp-workspace/spark-input-files/";
    SparkSession spark =
        SparkSession.builder()
            .appName("Simple App with Hive")
            .config("spark.sql.warehouse.dir", warehouseLocation)
            .enableHiveSupport()
            .getOrCreate();

    Dataset<Row> hvSql =
        spark.sql(
            "SELECT CustomerId, PartyId, DateOfBirth, FLOOR(datediff(current_date(),DateOfBirth)/365) Age FROM HiveCustomerTbl WHERE CustomerId>90");
    hvSql.show();

    hvSql.write().mode("overwrite").json("target/json");

    hvSql.write().partitionBy("PartyId").mode("overwrite").saveAsTable("HiveCustomerAgeTbl");

    hvSql
        .write()
        .format("csv")
        .mode("overwrite")
        .option("header", "true")
        .save("/users/tfatayo/cdp-workspace/customerage.csv");

    hvSql
        .write()
        .format("csv")
        .mode("overwrite")
        .option("header", "true")
        .save("s3a://my-emr-bucket1/my-hive-query-results/customerage.csv");

    spark.stop();
  }

  private static StructType buildSchema() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("CustomerId", DataTypes.IntegerType, true),
              DataTypes.createStructField("PartyId", DataTypes.IntegerType, false),
              DataTypes.createStructField("DateOfBirth", DataTypes.DateType, false),
              DataTypes.createStructField("FullName", DataTypes.StringType, false)
            });

    return (schema);
  }

  public void loadCsvAndSaveToHive(SparkSession spark, String filePath) {
    try {
      Dataset<Row> custDFCsv =
          spark
              .read()
              .schema(buildSchema())
              .option("dateFormat", "MM/dd/yy")
              .format("csv")
              .option("sep", ",")
              .option("header", "true")
              // .option("inferSchema", "true")
              .csv(filePath + "custDoB.csv");

      custDFCsv.select(col("CustomerId"), col("DateOfBirth")).show();
      custDFCsv.printSchema();

      custDFCsv.write().partitionBy("PartyId").mode("overwrite").saveAsTable("HiveCustomerTbl");

      Dataset<Row> hvSql = spark.sql("SELECT * FROM HiveCustomerTbl WHERE CustomerId>5");
      hvSql.show();

    } catch (Exception ex) {
      throw ex;
    }
  }

  public void processPI(SparkSession spark, String[] args) {

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
    int n = 100000 * slices;
    List<Integer> l = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      l.add(i);
    }

    JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);
    LocalDateTime startTime = LocalDateTime.now();
    int count =
        dataSet
            .map(
                new Function<Integer, Integer>() {
                  @Override
                  public Integer call(Integer integer) {
                    double x = Math.random() * 2 - 1;
                    double y = Math.random() * 2 - 1;
                    return (x * x + y * y < 1) ? 1 : 0;
                  }
                })
            .reduce(
                new Function2<Integer, Integer, Integer>() {
                  @Override
                  public Integer call(Integer integer, Integer integer2) {
                    return integer + integer2;
                  }
                });

    LocalDateTime endTime = LocalDateTime.now();
    Period period = new Period(startTime, endTime);
    System.out.println(
        "Pi is roughly "
            + 4.0 * count / n
            + " Time taken: "
            + period.getMillis()
            + " Slice no: "
            + slices);

    jsc.stop();
  }

  public void processFlatFileJob(SparkSession spark) throws AnalysisException {
    spark.sql(
        "CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive OPTIONS(fileFormat 'textfile')");
    // spark.sql("LOAD DATA LOCAL INPATH '/usr/local/Cellar/apache-spark/2.4.3/kv1' INTO TABLE
    // src");
    // spark.sql("LOAD DATA LOCAL INPATH '/spark-learning/src/main/resources/kv1' INTO TABLE src");
    spark.sql("SELECT * FROM src").show();
    System.out.println("---- Print the Total row count------");
    spark.sql("SELECT COUNT(*) from src").show();

    Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 100 ORDER BY key");
    Dataset<String> strDS =
        sqlDF.map(
            (MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
            Encoders.STRING());
    strDS.show();

    List<Record> records = new ArrayList<>();
    for (int key = 1; key <= 100; key++) {
      Record record = new Record();
      record.setKey(key);
      record.setValue("val_" + key);
      records.add(record);
    }
    Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
    recordsDF.createTempView("records");
    spark.sql("SELECT * FROM src s JOIN records r ON s.key==r.key").show();
  }

  public static class Record implements Serializable {
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
}
