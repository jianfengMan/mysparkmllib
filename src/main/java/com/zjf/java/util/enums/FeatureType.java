package com.zjf.java.util.enums;


/**
 * 特征类型
 */
public enum FeatureType {
    PROPERTY(1, "属性特征"),
    DETAIL(2, "明细特征");

    private int index;
    private String desc;

    private FeatureType(int index, String desc) {
        this.index = index;
        this.desc = desc;
    }

    public static FeatureType getType(int index) {
        FeatureType result = null;
        for (FeatureType type : values()) {
            if (type.getIndex() == index) {
                result = type;
                break;
            }
        }
        return result;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

}
