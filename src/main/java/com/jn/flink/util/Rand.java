package com.jn.flink.util;


import org.apache.commons.lang3.RandomStringUtils;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


public class Rand {


    public static long randLongRange(long min, long max) {
        Random random = new Random();
        long l = min + (long) (Math.random() * (max - min));
        return l;
    }

    public static int randIntRange(int min, int max) {
        Random random = new Random();
        int i = min + (int) (new Random().nextFloat() * (max - min));
        return i;
    }

    public static float randFloatRange(float min, float max) {
        float f = min + new Random().nextFloat() * (max - max);
        return f;
    }

    public static double randDoubleRange(double min, float max) {
        double d = min + new Random().nextFloat() * (max - max);
        return d;
    }

    public static String randString(int len) {
        String s = RandomStringUtils.randomAlphanumeric(len);
        return s;
    }

    public static LocalDate between(LocalDate startInclusive, LocalDate endExclusive) {
        long startEpochDay = startInclusive.toEpochDay();
        long endEpochDay = endExclusive.toEpochDay();
        long randomDay = ThreadLocalRandom
                .current()
                .nextLong(startEpochDay, endEpochDay);

        return LocalDate.ofEpochDay(randomDay);
    }


    public static LocalTime between(LocalTime startTime, LocalTime endTime) {
        int startSeconds = startTime.toSecondOfDay();
        int endSeconds = endTime.toSecondOfDay();
        int randomTime = ThreadLocalRandom
                .current()
                .nextInt(startSeconds, endSeconds);

        return LocalTime.ofSecondOfDay(randomTime);
    }


    public static <T extends Enum<?>> T randomEnum(Class<T> clazz, Integer min, Integer max) {
        Random random = new Random();
        if (min > clazz.getEnumConstants().length || max > clazz.getEnumConstants().length) {
            return null;
        }

        if (min != null && max != null) {
            int rand = random.nextInt(max - min + 1) + min - 1;
            return clazz.getEnumConstants()[rand];
        } else {
            int x = random.nextInt(clazz.getEnumConstants().length);
            return clazz.getEnumConstants()[x];
        }
    }
}
