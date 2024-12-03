package org.dhruv.utils;

import java.io.Serializable;

public class QueryAttribute implements Serializable {
    private String key;
    private String alias;
    private String condition;

    public QueryAttribute(String key, String alias, String condition){
        this.key = key;
        this.alias = alias;
        this.condition = condition;
    }


    public String getKey() {
        return key;
    }


    public void setKey(String key) {
        this.key = key;
    }


    public String getAlias() {
        return alias;
    }


    public void setAlias(String alias) {
        this.alias = alias;
    }


    public String getCondition() {
        return condition;
    }


    public void setCondition(String condition) {
        this.condition = condition;
    }


    public QueryAttribute(String key, String alias){
        this.key = key;
        this.alias = alias;
    }

    public QueryAttribute(){

    }

    @Override
    public String toString() {
       return String.format("QueryAttribute [%s,  %s,  %s]", key, alias, condition);
    }

    
    
    

}
