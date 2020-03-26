package cn.edu.nwsuaf.bloomfilter;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.nio.charset.Charset;

/**
 * @ClassName: BloomFilterDemo
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/26 3:50 下午
 */

public class BloomFilterDemo {
    public static void main(String[] args) {
        /**
         * 创建一个插入对象为一亿，误报率为0.01%的布隆过滤器
         * 利用guava实现的布隆过滤器
         * redis布隆过滤器以插件的形式
         */
        BloomFilter<CharSequence> bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.forName("utf-8")), 100000000, 0.0001);

        bloomFilter.put("hello");
        bloomFilter.put("world");
        bloomFilter.put("redis");

        System.out.println(bloomFilter.mightContain("hello"));
        System.out.println(bloomFilter.mightContain("Java"));

    }
}
