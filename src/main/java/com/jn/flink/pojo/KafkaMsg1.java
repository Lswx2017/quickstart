package com.jn.flink.pojo;

import kafka.Kafka;

public class KafkaMsg1 {
    public Long userId;
    public Long itemId;
    public Integer categoryId;
    public String behavior;
    public Long timestamp;


    public KafkaMsg1() {}

    public KafkaMsg1(Long uid, Long iid, Integer cid,String be, Long time) {
        userId = uid;
        itemId = iid;
        categoryId = cid;
        behavior = be;
        timestamp = time;

    }

    @Override
    public String toString() {
        return userId +
                "," + itemId +
                "," + categoryId +
                "," + behavior +
                "," + timestamp;
    }


}
