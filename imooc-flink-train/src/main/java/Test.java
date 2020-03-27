import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * create by liuzhiwei on 2020/3/25
 */
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入: 用户行为。某个用户在某个时刻点击或浏览了某个商品，以及商品的价格。
        DataStreamSource<UserAction> source = env.fromCollection(Arrays.asList(
                new UserAction("userID1", 1293984000, "click", "productID1", 10),
                new UserAction("userID2", 1293984001, "browse", "productID2", 8),
                new UserAction("userID1", 1293984002, "click", "productID1", 10)
        ));

        // 过滤: 过滤出用户ID为userID1的用户行为
        SingleOutputStreamOperator<UserAction> result = source.filter(new FilterFunction<UserAction>() {
            @Override
            public boolean filter(UserAction value) throws Exception {
                return value.getUserID().equals("userID1");
            }
        });

        // 输出: 输出到控制台
        // Test.UserAction(userID=userID1, eventTime=1293984002, eventType=click, productID=productID1, productPrice=10)
        // Test.UserAction(userID=userID1, eventTime=1293984000, eventType=click, productID=productID1, productPrice=10)
        result.print();

        env.execute("Test");

    }

    /**
     * create by liuzhiwei on 2020/3/25
     */

    public static class UserAction {
        private String userID;
        private long eventTime;
        private String eventType;
        private String productID;
        private int productPrice;

        public UserAction(String userID, long eventTime, String eventType, String productID, int productPrice) {
            this.userID = userID;
            this.eventTime = eventTime;
            this.eventType = eventType;
            this.productID = productID;
            this.productPrice = productPrice;
        }

        public String getUserID() {
            return userID;
        }

        public void setUserID(String userID) {
            this.userID = userID;
        }

        public long getEventTime() {
            return eventTime;
        }

        public void setEventTime(long eventTime) {
            this.eventTime = eventTime;
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public String getProductID() {
            return productID;
        }

        public void setProductID(String productID) {
            this.productID = productID;
        }

        public int getProductPrice() {
            return productPrice;
        }

        public void setProductPrice(int productPrice) {
            this.productPrice = productPrice;
        }
    }
}
