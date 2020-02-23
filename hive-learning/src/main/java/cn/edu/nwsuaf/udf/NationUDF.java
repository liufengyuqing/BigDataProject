package cn.edu.nwsuaf.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.HashMap;

/**
 * @ClassName: NationUDF
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/21 12:52 下午
 */

public class NationUDF extends UDF {
    Text text = new Text();

    public static HashMap<String, String> nationMap = new HashMap<>();

    static {
        nationMap.put("China", "中国");
        nationMap.put("U.S.A", "美国");
        nationMap.put("Japan", "日本");
    }

    /**
     * 重写evaluate方法
     *
     * @param nation
     * @return
     */
    public Text evaluate(Text nation) {
        String result = nationMap.get(nation.toString());
        if (result == null) {
            result = "未知";
        }
        text.set(result);
        return text;
    }

}
