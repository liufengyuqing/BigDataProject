package cn.edu.nwsuaf.udtf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

/**
 * @ClassName: ExplodeMap
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/21 4:49 下午
 */

public class ExplodeMap extends GenericUDTF {
    @Override
    public void process(Object[] args) throws HiveException {
        String input = args[0].toString();
        String[] strings = input.split(";");

        for (int i = 0; i < strings.length; i++) {
            try {
                String[] result = strings[i].split(":");
                forward(result);
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
        }

    }




    @Override
    public void close() throws HiveException {

    }

    public static void main(String[] args) throws HiveException {
        ExplodeMap explodeMap = new ExplodeMap();
        explodeMap.process(new String[]{"name:liuzhiwei;age:24"});
    }
}
