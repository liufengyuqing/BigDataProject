package flink.streaming.topN.HotGoodsTopN;

/**
 * create by liuzhiwei on 2020/6/14
 * POJO MyBehavior → 解析Json字符串后生成的JavaBean
 */
public class MyBehavior {
    public String userId;//用户ID
    public String itemId;//商品ID
    public String categoryId;//商品类目ID
    public String type;//用户行为，包括("pv", "buy", "cart", "fav")
    public long timestamp;//行为发生的时间戳，秒
    public long counts = 1;

    public static MyBehavior of(String userId, String itemId, String categoryId, String type, long timestamp) {
        MyBehavior behavior = new MyBehavior();
        behavior.userId = userId;
        behavior.itemId = itemId;
        behavior.categoryId = categoryId;
        behavior.type = type;
        behavior.timestamp = timestamp;
        return behavior;
    }

    public static MyBehavior of(String userId, String itemId, String categoryId, String type, long timestamp,
                                long counts) {
        MyBehavior behavior = new MyBehavior();
        behavior.userId = userId;
        behavior.itemId = itemId;
        behavior.categoryId = categoryId;
        behavior.type = type;
        behavior.timestamp = timestamp;
        behavior.counts = counts;
        return behavior;
    }



    @Override
    public String toString() {
        return "MyBehavior{" +
                "userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", type='" + type + '\'' +
                ", timestamp=" + timestamp +
                ", counts=" + counts +
                '}';
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getCounts() {
        return counts;
    }

    public void setCounts(long counts) {
        this.counts = counts;
    }
}
