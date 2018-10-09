package com.zjf.java.util.enums;


/**
 * 过滤类型
 */
public enum FilterType {

    EQUAL("e", "等于", "=%s"),
    NOTEQUAL("ne", "不等于", "!=%s"),
    LT("lt", "小于", "<%s"),
    GT("gt", "大于", ">%s"),
    CONTAIN("c", "包含", "%%s%"),
    BEGIN("begin", "开头是", "%s%"),
    END("end", "结尾是", "%%s"),
    IN("in", "在列表中", "in(%s)"),
    NOTIN("not_in", "不在列表中", "not in(%s)");


    private String index;
    private String desc;
    private String symbol;

    private FilterType(String index, String desc, String symbol) {
        this.index = index;
        this.desc = desc;
        this.symbol = symbol;
    }


    public static FilterType getType(String index) {
        FilterType result = null;
        for (FilterType type : values()) {
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

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }
}
