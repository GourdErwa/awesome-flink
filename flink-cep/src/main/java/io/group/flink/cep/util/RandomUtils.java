package io.group.flink.cep.util;

import java.text.DecimalFormat;
import java.util.Random;

/**
 * https://gitee.com/tang006/mock/blob/master/src/main/java/com/mock/mocker/EnumMocker.java
 *
 * @author Li.Wei by 2021/12/24
 */
public class RandomUtils {
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#####.0000");
    private final static Random RANDOM = new Random();

    private RandomUtils() {
    }

    public static boolean nextBoolean() {
        return RANDOM.nextBoolean();
    }

    public static int nextInt(int startInclusive, int endExclusive) {
        return startInclusive + RANDOM.nextInt(endExclusive - startInclusive);
    }

    public static long nextLong(long startInclusive, long endExclusive) {
        return (long) nextDouble(startInclusive, endExclusive);
    }

    public static float nextFloat(float startInclusive, float endInclusive) {
        return Float.parseFloat(DECIMAL_FORMAT.format(startInclusive + ((endInclusive - startInclusive) * RANDOM.nextFloat())));
    }

    public static double nextDouble(double startInclusive, double endInclusive) {
        return Double.parseDouble(DECIMAL_FORMAT.format(startInclusive + ((endInclusive - startInclusive) * RANDOM.nextDouble())));
    }

    public static int nextSize(int startInclusive, int endInclusive) {
        return startInclusive + RANDOM.nextInt(endInclusive - startInclusive + 1);
    }
}
