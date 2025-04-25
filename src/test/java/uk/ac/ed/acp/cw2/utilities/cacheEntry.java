package uk.ac.ed.acp.cw2.utilities;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

public class cacheEntry {
    @JsonProperty("key")
    @Getter
    String key;
    @JsonProperty("entry")
    @Getter
    String entry;

    public String toString(){
        return "Key: " + key + ", Entry: " + entry;
    }
}
