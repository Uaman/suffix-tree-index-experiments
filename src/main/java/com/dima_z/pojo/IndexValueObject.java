package com.dima_z.pojo;

public class IndexValueObject {
    
    public String id = null;
    public String name = null;

    public IndexValueObject() {
    }

    public IndexValueObject(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public IndexValueObject(String[] recordRow) {
        this.id = recordRow[0];
        this.name = recordRow[1];
    }
}
