package flink.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * create by liuzhiwei on 2020/4/4
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WC {
    private String word;
    private int nums;
}
