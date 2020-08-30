package com.jn.flink.enums;

public enum Ecommenum{

    PV("1",  "PV", "浏览商品"),

    FAV("2", "FAV", "收藏商品"),

    BUY("3",  "BUY", "购买商品"),

    CART("4",  "CART", "加入购物车");

    private String id;

    private String status;

    private String desc;

    Ecommenum(String id, String status, String desc) {
        this.id = id;
        this.status = status;
        this.desc = desc;
    }

    public static String getDescByStatus(String status) {
        if (status == null) {
            return null;
        }
        for (Ecommenum ecommenum : Ecommenum.values()) {
            if (ecommenum.getStatus().equals(status)) {
                return ecommenum.getDesc();
            }
        }
        return null;
    }

    public String getId() {
        return id;
    }

    public String getStatus() {
        return status;
    }

    public String getDesc() {
        return desc;
    }
}
