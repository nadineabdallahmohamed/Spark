package spark;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Count1 {
    private static final String COMMA_DELIMITER = ",";
    public static void getCount1() throws IOException {
        Logger.getLogger ("org").setLevel (Level.ERROR);
    
        SparkConf sp = new SparkConf ().setAppName ("Count1").setMaster ("local[3]");
        JavaSparkContext cnt = new JavaSparkContext (sp);
     
        JavaRDD<String> s = cnt.textFile ("USvideos.csv");
 
   

        JavaRDD<String> st = s
                .map (Count1::getL)
                .filter (StringUtils::isNotBlank);

        JavaRDD<String> m = st.flatMap (title -> Arrays.asList (title
                .toLowerCase ()
                .trim ()
                .replaceAll ("\\p{Punct}", " ")
               .split (" ")).iterator ());
        System.out.println( m.toString ());

        Map<String, Long> mp = m.countByValue ();
        List<Map.Entry> lst = mp.entrySet ().stream ()
                .sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
        for (Map.Entry<String, Long> entry : lst) {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
    }
    private static String getL(String s) {
        try {
            return s.split (COMMA_DELIMITER)[2];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
        
    }
}
