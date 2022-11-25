package org.flinkwordcount.consumer;

import java.sql.Timestamp;

public class Tag {
    public String tag;
    public Long timestamp;

    public Tag(){}
    public Tag(String tag, Long timestamp) {
        this.tag = tag;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Tag{" +
                "tag='" + tag + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
