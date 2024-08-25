package com.dima_z.indexes.testCases;

public enum QueryType {
    SEARCH("searchQuery"),
    INCLUDES("includesQuery"),
    NOT_INCLUDES("notIncludesQuery"),
    STARTS_WITH("startsWithQuery"),
    NOT_STARTS_WITH("notStartsQuery"),
    ENDS_WITH("endsWithQuery"),
    NOT_ENDS_WITH("notEndsQuery");

    private final String type;

    QueryType(String val) {
        this.type = val;
    }

    public String getType() {
        return type;
    }
}
