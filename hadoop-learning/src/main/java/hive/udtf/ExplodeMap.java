package hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

/**
 * @ClassName: ExplodeMap
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/21 4:49 下午
 * <p>
 * https://www.cnblogs.com/ggjucheng/archive/2013/02/01/2888819.html
 */

public class ExplodeMap extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 1) {
            throw new UDFArgumentLengthException("ExplodeMap takes only one argument");
        }
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("ExplodeMap takes string as a parameter");
        }
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("col1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("col2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

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
