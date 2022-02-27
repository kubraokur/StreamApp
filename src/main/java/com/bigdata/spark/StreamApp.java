package com.bigdata.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class StreamApp {

    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
        SparkSession session=SparkSession.builder().appName("Streaming").master("local").getOrCreate();

      try {

            // Dosya kontrol
            File f = new File("C:\\Users\\maviyekubra.okur\\Desktop\\StreamPerson.csv");

            // Yeni Dosya Oluştur
            // Dosyanın olup olmadığını kontrol edip oluştur.
            if (f.createNewFile())
                System.out.println("Dosya Oluşturuldu");
            else
                System.out.println("Dosya Zaten var!");
        }
        catch (IOException e) {
            System.err.println(e);
        }

        StructType personType=new StructType().add("firstName", DataTypes.StringType).add("lastName", DataTypes.StringType).add("mail", DataTypes.StringType).add("gender", DataTypes.StringType).add("country", DataTypes.StringType);
        Dataset<Row> rawData = session.readStream().schema(personType).option("sep", ",").format("csv").load("file:///C:\\Users\\maviyekubra.okur\\Desktop\\test\\*.txt");

        StreamingQuery start = rawData.writeStream().format("csv").option("sep", ",").option("path","file:///C:\\Users\\maviyekubra.okur\\Desktop\\write\\yyyy.csv").option("checkpointLocation","C:\\Users\\maviyekubra.okur\\Desktop\\checkpoint").start();

        // StreamingQuery start = rawData.writeStream().outputMode("complete").format("console").start();




        start.awaitTermination();
    }
}
