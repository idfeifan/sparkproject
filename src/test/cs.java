import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: Liufeifan
 * @Date: 2020/12/17 22:03
 */
public class cs {
    public static void main(String[] args) {
        ArrayList<String> studentsWithNameAge = new ArrayList<String>();
        studentsWithNameAge.add("(\"leo\",23)");
        studentsWithNameAge.add("(\"jack\",35)");
        List<String> ss = Arrays.asList("(\"leo\",23)","(\"jack\",35)");
        System.out.println(ss);
        System.out.println(ss.get(1));
    }
}
