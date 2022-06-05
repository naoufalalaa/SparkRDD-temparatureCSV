import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class ApplicationTD2 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("word count").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=sc.textFile("ventes.txt");
        JavaPairRDD<String,Double> rdd3=rdd1.mapToPair(s->new Tuple2<>(s.split(";")[1],Double.parseDouble(s.split(";")[3])));
        JavaPairRDD<String,Double> rdd4=rdd3.reduceByKey((v1, v2)->v1+v2);

        System.out.println("Vente pour villes :");
        rdd4.foreach(s->System.out.println(s._1+" "+s._2));
        System.out.println("-----------------------------------------------------");

        JavaPairRDD<String,Double> rdd5=rdd1.mapToPair(s->new Tuple2<>(s.split(";")[0]+" "+s.split(";")[1],Double.parseDouble(s.split(";")[3])));
        JavaPairRDD<String,Double> rdd6=rdd5.reduceByKey((v1, v2)->v1+v2);
        System.out.println("Vente pour villes en années :");
        rdd6.foreach(s->System.out.println(s._1+" "+s._2));
        System.out.println("-----------------------------------------------------");
    }
}
