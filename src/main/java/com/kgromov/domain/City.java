package com.kgromov.domain;

public enum City {
    ODESSA("odessa", 111, "погода-одеса");

    private final String name;
    private final int code;

    private final String keyWord;

    City(String name, int code, String keyWord) {
        this.name = name;
        this.code = code;
        this.keyWord = keyWord;
    }

    public String getName() {
        return name;
    }

    public int getCode() {
        return code;
    }

    public String getKeyWord() {
        return keyWord;
    }

    @Override
    public String toString() {
        return getName();
    }
}
