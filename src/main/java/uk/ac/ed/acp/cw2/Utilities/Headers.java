package uk.ac.ed.acp.cw2.Utilities;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

public class Headers {
    public static HttpHeaders createJsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }
}
