package spark;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class Count2 {
    
      private static final String Pipe_DELIMITER = "|";
      private static final String  comma_DELIMITER = ",";

    public static void getCount2() throws IOException {
        Logger.getLogger ("org").setLevel (Level.ERROR);

        SparkConf sp = new SparkConf ().setAppName ("Count2").setMaster ("local[6]");
        JavaSparkContext cnt = new JavaSparkContext (sp);
   
        JavaRDD<String> out = cnt.textFile ("USvideos.csv");
 

        JavaRDD<String> m = out
                .map (Count2::getT)
                .filter (StringUtils::isNotBlank);
        JavaRDD<String> n = m.flatMap (tag -> Arrays.asList (tag
                .toLowerCase ()
                .trim ()
                .replaceAll ("\\p{Punct}", " ")
               .split (" ")).iterator ());
        System.out.println( n.toString ());

        Map<String, Long> mp = n.countByValue ();
        List<Map.Entry> lst = mp.entrySet ().stream ()
                .sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());

        for (Map.Entry<String, Long> entry : sorted) {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
    }
    private static String getT(String s) {
        try {
             String value =  s.split (comma_DELIMITER)[6];
             return  value.split(comma_DELIMITER)[0];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
        
    }
}
