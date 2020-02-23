package cn.edu.nwsuaf.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluatorResolver;

/**
 * @ClassName: DemoUTAF
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/22 4:10 下午
 */

public class DemoUTAF extends UDAF {
    public DemoUTAF() {
        super();
    }

    public DemoUTAF(UDAFEvaluatorResolver rslv) {
        super(rslv);
    }

    @Override
    public void setResolver(UDAFEvaluatorResolver rslv) {
        super.setResolver(rslv);
    }

    @Override
    public UDAFEvaluatorResolver getResolver() {
        return super.getResolver();
    }
}
