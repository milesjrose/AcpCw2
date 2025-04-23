package uk.ac.ed.acp.cw2.utilities;

import java.util.Random;

public class RandomGenerator {
    static Random random = new Random();

    public static Float generateFloat(Integer upperBound, Integer lowerBound){
        //generate a random float
        return lowerBound + random.nextFloat() * (upperBound - lowerBound);
    }

    public static String generateString(Integer length){
        //generate a random string of a given length
        return random.ints(48, 122)
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
            .mapToObj(i -> (char) i)
            .limit(length)
            .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
            .toString();
    }

    public static Integer generateInteger(Integer upperBound, Integer lowerBound){
        //generate a random integer
        return lowerBound + random.nextInt(upperBound - lowerBound);
    }

    public static Boolean generateBoolean(){
        //generate a random boolean
        return random.nextBoolean();
    }

    public static String generateRandomKey(String prefix) {
        // Generate a random 5-digit number for the first part
        int numericPart = 10000 + random.nextInt(90000);

        // Generate a random 5-character alphanumeric string for the second part
        String alphanumericChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder alphanumericPart = new StringBuilder();

        for (int i = 0; i < 5; i++) {
            int index = random.nextInt(alphanumericChars.length());
            alphanumericPart.append(alphanumericChars.charAt(index));
        }

        // Combine the parts with a hyphen
        return prefix + "-" + numericPart + "-" + alphanumericPart.toString();
    }
    

}
