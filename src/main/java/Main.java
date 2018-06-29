//https://vk.com/dev/audio_api was closed on 16 dec 2016

//https://vk.com/audios289138434



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import scala.Tuple2;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Main {
    static final List<String> badWords = Arrays.asList("и","не","в","the","на","i","что","to","and","a","а","по","in","за","у","of","с","но","как","из","no","не","от","for","on","is","it","только","о");//потом DI через Spring
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("my spark application");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> mainRdd = sc.textFile("C:\\CommonWords\\src\\main\\resources\\kek.txt")//придумать как json парсить , запятые в названии все портят
                .map(x->x.replaceAll("\\s+|\"", " ").trim())
                .flatMap(x-> Arrays.asList(x.split(",")).iterator()).map(Main::getText); //split ", ?
        List<Tuple2<String,Integer>> words = mainRdd.flatMap(x-> Arrays.asList(x.split(" ")).iterator())
                .filter(x->!badWords.contains(x))
                .mapToPair(w -> new Tuple2<>(w, 1))
                .reduceByKey((a, b) -> a + b)
                .mapToPair(Tuple2::swap)//двойной свап , вы серьезно?
                .sortByKey(false)//ждешь 10 минут потом узнаешь что нужно передать false
                .mapToPair(Tuple2::swap)
                .take(50);
        words.forEach(System.out::println);

    }
    static String getText(String S){
        String text="";
        try {
            Document doc = Jsoup.connect("http://www.megalyrics.ru/search?utf8=✓&search="+S.replace(" ","+")).get();
            String mainUrl = doc.select(".songs-table > table > tbody > tr > .st-title > a").first().attr("href");
            Document docA = Jsoup.connect("http://www.megalyrics.ru/"+mainUrl).get();
            System.out.println(S+" найдена "+mainUrl);
            text = docA.select(".text_inner").first().text().toLowerCase().replaceAll("\\pP", "");//откуда запятые то?
        } catch (IOException | NullPointerException e) {
            System.out.println("Не найдена песня "+S);
        }

        return text;
    }
}
