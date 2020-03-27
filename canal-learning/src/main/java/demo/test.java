package demo;


import com.alibaba.otter.canal.common.utils.AddressUtils;

/**
 * create by liuzhiwei on 2020/3/23
 */
public class test {
    public static void main(String[] args) {
        String ip = AddressUtils.getHostIp();
        System.out.println(ip);
    }
}
