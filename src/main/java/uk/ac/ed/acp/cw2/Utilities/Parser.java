package uk.ac.ed.acp.cw2.Utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Parser {
    private static final Logger logger = LoggerFactory.getLogger(Parser.class);

    public static String parseString(String recString) {
        if (recString == null) {
            logger.error("Null string received");
            return null;
        } else if (recString.startsWith("\"") && recString.endsWith("\"")) {
            return recString.substring(1, recString.length() - 1);
        } else {
            return recString;
        }
    }
}
