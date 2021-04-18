package ldbc.snb.datagen.serializer.yarspg;

import ldbc.snb.datagen.serializer.yarspg.property.PrimitiveType;

import java.util.List;
import java.util.stream.Collectors;

public class Wrapper {
    public static String wrap(String arg) {
        return "\"" + arg + "\"";
    }

    public static String wrap(String arg, Bracket bracket) {
        char leftBracket = bracket.toString()
                .charAt(0);
        char rightBracket = bracket.toString()
                .charAt(1);

        return leftBracket + arg + rightBracket;
    }

    public static String wrapWithQuote(String arg, Bracket bracket) {
        char leftBracket = bracket.toString()
                .charAt(0);
        char rightBracket = bracket.toString()
                .charAt(1);

        return leftBracket + wrap(arg) + rightBracket;
    }

    public static String wrapSet(String arg) {
        char leftBracket = Bracket.CURLY.toString()
                .charAt(0);
        char rightBracket = Bracket.CURLY.toString()
                .charAt(1);

        return leftBracket + arg + rightBracket;
    }

    public static String wrapList(String arg) {
        char leftBracket = Bracket.SQUARE.toString()
                .charAt(0);
        char rightBracket = Bracket.SQUARE.toString()
                .charAt(1);

        return leftBracket + arg + rightBracket;
    }

    public static String wrapStruct(String key, String value) {
        char leftBracket = Bracket.CURLY.toString()
                .charAt(0);
        char rightBracket = Bracket.CURLY.toString()
                .charAt(1);

        return leftBracket + wrapProperty(key, value) + rightBracket;
    }

    public static String wrapProperty(String key, String value, PrimitiveType primitiveType) {
        String propValue = shouldBeQuoted(primitiveType) ? wrap(value) : value;

        return wrap(key) + ":" + propValue;
    }

    public static String wrapProperties(List<String> values, PrimitiveType primitiveType) {
        if (shouldBeQuoted(primitiveType)) {
            return values.stream()
                    .map(Wrapper::wrap)
                    .collect(Collectors.joining(", "));
        }

        return String.join(", ", values);
    }

    public static String wrapProperty(String key, String value) {
        return wrap(key) + ":" + value;
    }

    public static String wrap(String key, String value) {
        return key + ":" + value;
    }

    private static boolean shouldBeQuoted(PrimitiveType primitiveType) {
        return primitiveType.equals(PrimitiveType.STRING);
    }


    public enum Bracket {
        ROUND("()"),
        SQUARE("[]"),
        CURLY("{}"),
        ANGLE("<>"),
        SLASH("//"),
        QUOTE("\"\"");

        private final String name;

        Bracket(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }
}
