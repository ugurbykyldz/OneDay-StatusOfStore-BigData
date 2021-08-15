import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Application {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master");
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder().master("local")
                .appName("magaza").getOrCreate();


        Dataset<Row> loadDS = sparkSession.read().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "magaza").load().selectExpr("CAST(value as STRING)");

        //loadDS.show();

        StructType schema = new StructType()
                .add("userID", DataTypes.IntegerType)
                .add("product", DataTypes.StringType)
                .add("price", DataTypes.IntegerType)
                .add("ptype", DataTypes.StringType)
                .add("time", DataTypes.TimestampType);


        Dataset<Row> rawDS = loadDS.select(functions.from_json(loadDS.col("value"), schema).as("data")).select("data.*");

        /*
        rawDS.show(5, false);
        +------+---------+-----+------------+-------------------+
        |userID|product  |price|ptype       |time               |
        +------+---------+-----+------------+-------------------+
        |1433  |Kot Ceket|79   |Post        |2021-08-15 10:56:28|
        |1240  |Şort     |189  |Nakit       |2021-08-15 10:15:52|
        |353   |Bluz     |183  |Nakit       |2021-08-15 00:45:14|
        |173   |Saat     |334  |Hediye Kartı|2021-08-15 05:44:32|
        |1961  |Elbise   |105  |Post        |2021-08-15 19:41:02|
        +------+---------+-----+------------+-------------------+
        */
        /*
        rawDS.printSchema();
         |-- userID: integer (nullable = true)
         |-- product: string (nullable = true)
         |-- price: integer (nullable = true)
         |-- ptype: string (nullable = true)
         |-- time: timestamp (nullable = true)
         */


        /*
        rawDS.describe().show(false);
        +-------+------------------+-------+------------------+------------+
        |summary|userID            |product|price             |ptype       |
        +-------+------------------+-------+------------------+------------+
        |count  |16149             |16149  |16149             |16149       |
        |mean   |1002.1948727475385|null   |198.82159886061058|null        |
        |stddev |578.7345262113263 |null   |100.60119536781126|null        |
        |min    |1                 |Bluz   |25                |Hediye Kartı|
        |max    |2000              |Şort   |374               |Post        |
        +-------+------------------+-------+------------------+------------+

         */


        // ----------EN ÇOK SATAN 5 ÜRÜN--------------
       Dataset<Row> enCokSatanUrun = rawDS.groupBy(rawDS.col("product")).count().sort(functions.desc("count"));

        /*
        enCokSatanUrun.show(5 , false);
        +------------+-----+
        |product     |count|
        +------------+-----+
        |Kot Pantolon|1051 |
        |Bluz        |990  |
        |Tesettür    |977  |
        |Plaj Giyim  |958  |
        |Çanta       |955  |
        +------------+-----+
         */

        //-----------GÜNÜN SAATLERİNE GÖRE  SATILAN ÜRÜN SAYISI------------

       Dataset<Row> gunuSaatUrunAdedi = rawDS.groupBy(functions.window(rawDS.col("time"), "1 hour"), rawDS.col("product")).count()
                .groupBy("window").pivot("product").sum("count");

        /*
        gunuSaatUrunAdedi.show(5, false);
        +------------------------------------------+----+-----+------+----+------+---------------+---------+------------+--------+----------+----+-------------+----+--------+------+-----+----+
        |window                                    |Bluz|Ceket|Elbise|Etek|Gömlek|Günlük Ayakkabı|Kot Ceket|Kot Pantolon|Pantolon|Plaj Giyim|Saat|Spor Ayakkabı|Takı|Tesettür|Tişört|Çanta|Şort|
        +------------------------------------------+----+-----+------+----+------+---------------+---------+------------+--------+----------+----+-------------+----+--------+------+-----+----+
        |[2021-08-15 20:00:00, 2021-08-15 21:00:00]|50  |50   |39    |36  |39    |39             |33       |49          |39      |51        |39  |36           |39  |35      |35    |39   |41  |
        |[2021-08-15 21:00:00, 2021-08-15 22:00:00]|36  |43   |42    |41  |58    |39             |39       |48          |37      |45        |31  |53           |40  |47      |38    |32   |37  |
        |[2021-08-15 06:00:00, 2021-08-15 07:00:00]|44  |40   |41    |35  |43    |36             |38       |47          |45      |40        |41  |38           |40  |39      |39    |46   |38  |
        |[2021-08-15 11:00:00, 2021-08-15 12:00:00]|43  |47   |37    |41  |56    |40             |33       |56          |30      |39        |51  |38           |40  |41      |29    |40   |40  |
        |[2021-08-15 03:00:00, 2021-08-15 04:00:00]|40  |31   |29    |40  |40    |47             |32       |40          |45      |43        |34  |33           |42  |38      |36    |39   |31  |
        +------------------------------------------+----+-----+------+----+------+---------------+---------+------------+--------+----------+----+-------------+----+--------+------+-----+----+
         */

        //-------------GÜNÜN EN YOGUN SAATLERİ------------

        Dataset<Row> gunuEnYogunSaatMusteri = rawDS.groupBy(functions.window(rawDS.col("time"), "1 hour"), rawDS.col("userID")).count()
               .groupBy("window").sum("count").sort(functions.desc("sum(count)"));

        Dataset<Row> gunuEnYogunSaati = gunuEnYogunSaatMusteri .withColumn("toplamMusteri", gunuEnYogunSaatMusteri.col("sum(count)")).drop(gunuEnYogunSaatMusteri.col("sum(count)"));

        /*
        gunuEnYogunSaati.show(5 , false);
        +------------------------------------------+-------------+
        |window                                    |toplamMusteri|
        +------------------------------------------+-------------+
        |[2021-08-15 19:00:00, 2021-08-15 20:00:00]|708          |
        |[2021-08-15 21:00:00, 2021-08-15 22:00:00]|706          |
        |[2021-08-15 11:00:00, 2021-08-15 12:00:00]|701          |
        |[2021-08-15 09:00:00, 2021-08-15 10:00:00]|701          |
        |[2021-08-15 06:00:00, 2021-08-15 07:00:00]|690          |
        +------------------------------------------+-------------+
         */


        //--------------EN FAZLA ÜRÜN ALAN MÜŞTERİLER VE ÖDEDİGİ TUTARLAR------------

        Dataset<Row> enFazlaUrunAlan_fiyat = rawDS.groupBy("userID", "product", "price").count()
                .groupBy("userID").sum("count", "price").sort(functions.desc("sum(count)"));


        Dataset<Row> enFazlaUrunAlan_fiyat_final= enFazlaUrunAlan_fiyat.withColumnRenamed("sum(count)", "adet").withColumnRenamed("sum(price)", "tutar");

        /*
        enFazlaUrunAlan_fiyat_final.show(5 , false);
        +------+----+-----+
        |userID|adet|tutar|
        +------+----+-----+
        |1030  |18  |4520 |
        |1799  |18  |3426 |
        |1581  |17  |3537 |
        |511   |17  |2723 |
        |358   |17  |2735 |
        +------+----+-----+
         */


        //------------SAATLİK YAPILAN ÖDEME TÜRLERİ----------------

        Dataset<Row> saatlikOdemeturleri = rawDS.groupBy(functions.window(rawDS.col("time"), "1 hour"), rawDS.col("ptype")).count()
              .groupBy("window").pivot("ptype").sum("count").na().fill(0);


        /*
        saatlikOdemeturleri.show(5 , false);
        +------------------------------------------+------------+-----+----+
        |window                                    |Hediye Kartı|Nakit|Post|
        +------------------------------------------+------------+-----+----+
        |[2021-08-15 20:00:00, 2021-08-15 21:00:00]|226         |234  |229 |
        |[2021-08-15 06:00:00, 2021-08-15 07:00:00]|247         |228  |215 |
        |[2021-08-15 21:00:00, 2021-08-15 22:00:00]|246         |210  |250 |
        |[2021-08-15 11:00:00, 2021-08-15 12:00:00]|237         |219  |245 |
        |[2021-08-15 03:00:00, 2021-08-15 04:00:00]|224         |209  |207 |
        +------------------------------------------+------------+-----+----+
         */




        //-------SAVE HDFS---------------

        enCokSatanUrun.coalesce(1).write().csv("hdfs://localhost:8020/data/enCokSatanUrun");

        gunuSaatUrunAdedi.coalesce(1).write().csv("hdfs://localhost:8020/data/gunuSaatUrunAdedi");

        gunuEnYogunSaati.coalesce(1).write().csv("hdfs://localhost:8020/data/gunuEnYogunSaati");

        enFazlaUrunAlan_fiyat_final.coalesce(1).write().csv("hdfs://localhost:8020/data/enFazlaUrunAlan_fiyat_final");

        saatlikOdemeturleri.coalesce(1).write().csv("hdfs://localhost:8020/data/saatlikOdemeturleri");


    }
}
