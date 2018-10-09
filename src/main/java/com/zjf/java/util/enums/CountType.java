package com.zjf.java.util.enums;


/**
 * 计数类型
 */
public enum CountType {

    COUNT("count", "计数"),
    DISTINCT("distinct", "去重"),
    SUM("sum", "求和"),
    AVG("avg", "求平均"),
    MAX("max", "求最大值"),
    MIN("min", "求最小值");

    private String index;
    private String desc;

    private CountType(String index, String desc) {
        this.index = index;
        this.desc = desc;
    }


    public static CountType getType(String index) {
        CountType result = null;
        for (CountType type : values()) {
            if (type.getIndex().equals(index)) {
                result = type;
                break;
            }
        }
        return result;
    }


    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

}
