package com.example.flink.common.model;

import java.util.Objects;

/**
 * 贯穿整个学习项目的核心数据模型：用户点击事件。
 *
 * <p>字段说明：
 * <ul>
 *   <li>user：用户名（用于 keyBy 分组）</li>
 *   <li>url ：访问的页面地址</li>
 *   <li>ts  ：事件发生的时间戳（毫秒），用于演示 EventTime / 水位线</li>
 * </ul>
 *
 * <p>注意：Flink 中作为数据流传输的 POJO 需要满足：
 * 1) 类是 public 的；2) 有 public 的无参构造方法；3) 字段可访问（public 或有 getter/setter）。
 */
public class Event {

    private String user;
    private String url;
    private Long ts;

    public Event() {
    }

    public Event(String user, String url, Long ts) {
        this.user = user;
        this.url = url;
        this.ts = ts;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Event that = (Event) o;
        return Objects.equals(user, that.user)
                && Objects.equals(url, that.url)
                && Objects.equals(ts, that.ts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, url, ts);
    }

    @Override
    public String toString() {
        return "Event{user='" + user + "', url='" + url + "', ts=" + ts + "}";
    }
}
