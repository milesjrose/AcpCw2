package uk.ac.ed.acp.cw2.utilities;

import lombok.Getter;

public class cacheEntry {
    @Getter
    String key;
    @Getter
    String entry;

    public String toString(){
        return "Key: " + key + ", Entry: " + entry;
    }
}
