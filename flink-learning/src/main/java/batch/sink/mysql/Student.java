package batch.sink.mysql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * create by liuzhiwei on 2020/3/31
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Student {
    private int id;
    private String name;
    private int age;
    private String sex;
}
