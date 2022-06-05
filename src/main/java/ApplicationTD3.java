import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Collections;
import java.util.Map;

public class ApplicationTD3 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("Temperature").setMaster("local[*]"); // local[*] pour avoir un nombre de thread égal au nombre de coeurs
        JavaSparkContext sc=new JavaSparkContext(conf); // création du contexte spark

        //Moyenne de la température maximale
        JavaRDD<String> rdd1=sc.textFile("1750.csv"); //RDD de type String
        JavaRDD<String> rdd2= rdd1.filter(line -> line.split(",")[2].equals("TMAX")); //on ne garde que les lignes qui contiennent TMAX
        JavaPairRDD<String,Integer> rdd3=rdd2.mapToPair( //mapToPair permet de transformer un RDD en un RDD de paires
                s->new Tuple2<>(s.split(",")[2]// on récupère la valeur de la deuxième colonne (TMAX)
                        ,Integer.parseInt(s.split(",")[3]))//on récupère la valeur de la température
        );
        JavaPairRDD<String,Integer> rdd4=rdd3.reduceByKey((a,b)->a+b); //on réduit les valeurs par la méthode reduceByKey
        Long nbr= rdd2.count(); //nombre de lignes
        rdd4.foreach(s-> System.out.println("Moyenne des temperatures maximale : "+s._1+" "+s._2/nbr)); //on affiche la moyenne

        //Moyenne de la température minimale
        JavaRDD<String> rdd5= rdd1.filter(line -> line.split(",")[2].equals("TMIN")); //on ne garde que les lignes qui contiennent TMIN
        JavaPairRDD<String,Integer> rdd6=rdd5.mapToPair(s->new Tuple2<>(s.split(",")[2],Integer.parseInt(s.split(",")[3]))); //mapToPair permet de transformer un RDD en un RDD de paires
        JavaPairRDD<String,Integer> rdd7=rdd6.reduceByKey((a,b)->a+b); //on réduit les valeurs par la méthode reduceByKey
        Long nbr2= rdd5.count();
        rdd7.foreach(s-> System.out.println("Moyenne des temperatures minimale : "+s._1+" "+s._2/nbr2));

        //Température maximale
        JavaPairRDD<String,Integer> rdd9=rdd2.mapToPair(s->new Tuple2<>(s.split(",")[2],Integer.parseInt(s.split(",")[3])));
        JavaPairRDD<String,Integer> rdd10=rdd9.reduceByKey((a,b)->Math.max(a,b));
        rdd10.foreach(s-> System.out.println("Temperature maximale : "+s._1+" "+s._2));

        //température Maximale minimum
        JavaPairRDD<String,Integer> rdd11=rdd2.mapToPair(s->new Tuple2<>(s.split(",")[2],Integer.parseInt(s.split(",")[3])));
        JavaPairRDD<String,Integer> rdd12=rdd11.reduceByKey((a,b)->Math.min(a,b));
        rdd12.foreach(s-> System.out.println("Temperature Maximale minimum : "+s._1+" "+s._2));

        //Top 5 des stations les plus froides
        JavaPairRDD<String,Integer> rdd15=rdd5.mapToPair(s->new Tuple2<>(s.split(",")[0],Integer.parseInt(s.split(",")[3])));
        JavaPairRDD<String,Integer> rdd16=rdd15.reduceByKey((a,b)->Math.min(a,b));
        //List<Tuple2<String, Integer>> rdd17=rdd16.sortByKey().take(5);
        //rdd17.forEach(s-> System.out.println("Top 5 des stations les plus froides : "+s._1+" "+s._2));
        Map<String,Integer> _rdd18=rdd16.collectAsMap();
        _rdd18.entrySet() // On récupère la liste des entrées
                .stream() // On transforme la liste en flux
                .sorted(Map.Entry.comparingByValue()) // On trie les entrées par valeur
                .limit(5) // On limite le nombre d'entrées à 5
                .forEach(s-> System.out.println("Top 5 des stations les plus froides : "
                        +s.getKey()+" "+s.getValue()) // On affiche les entrées
                )
        ;

        //Top 5 des stations les plus chaudes
        JavaPairRDD<String,Integer> rdd18=rdd2.mapToPair(s->new Tuple2<>(s.split(",")[0],Integer.parseInt(s.split(",")[3]))); //on récupère les stations et les températures
        JavaPairRDD<String,Integer> rdd19=rdd18.reduceByKey((a,b)->Math.max(a,b));
        Map<String,Integer> rdd21=rdd19.collectAsMap(); // Pour pouvoir maper les valeurs entière et les comparer ensuite
        rdd21.entrySet() // On récupère les entrées
                .stream()// On convertit la map en stream
                .sorted(Collections.reverseOrder( // On trie les entrées par ordre décroissant
                        Map.Entry.comparingByValue() // On compare les valeurs
                ) ) // On trie les entrées par valeur
                .limit(5) // On limite le nombre de résultats
                .forEach(s-> System.out.println("Top 5 des stations les plus chaudes : "
                        +s.getKey()+" "+s.getValue()) // On affiche les résultats
                )
        ;

    }
}
