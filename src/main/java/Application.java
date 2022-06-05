import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class Application {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("TP 1 RDD").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<Integer> rdd1=sc.parallelize(Arrays.asList(12,10,9,20,39,8));
        JavaRDD<Integer> rdd2=rdd1.map(a -> a+1);
        JavaRDD<Integer> rdd3=rdd2.filter(a -> a>10);
        JavaRDD<Integer> rdd4=rdd1.union(rdd2);
        //affichqge de RDD4
        rdd4.foreach(a-> System.out.println(a));
        //somme des elements de RDD3
        int somme=rdd3.reduce((a, b) ->a+b );
        System.out.println(somme);

    }
}
