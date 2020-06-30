package batch.sink.mysql;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * create by liuzhiwei on 2020/4/1
 */
public class StudentDeserializationSchema implements DeserializationSchema<Student> {
    @Override
    public Student deserialize(byte[] message) throws IOException {
        return (Student) JSON.parseObject(new String(message), new TypeReference<Student>() {
        });
    }

    @Override
    public boolean isEndOfStream(Student nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Student> getProducedType() {
        return TypeInformation.of(new TypeHint<Student>() {
        });
    }
}
