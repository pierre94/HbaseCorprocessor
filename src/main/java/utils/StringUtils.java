package utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
    // A non-capture group so that this can be embedded.
    public static final String VALID_OBSERVER_REGEX = "([a-zA-Z][a-zA-Z0-9-]*)";
    public static Pattern p = Pattern.compile(VALID_OBSERVER_REGEX);

    public static boolean hasString(String[] strings, String target) {
        for (String s : strings) {
            if (s.equals(target)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isLegalObserverName(String name) {
        return p.matcher(name).matches();
    }

    public static boolean isStringEqual(String a, String b) {
        if (a == null && b == null) {
            return true;
        }
        if (a != null && b != null && a.equals(b)) {
            return true;
        }
        return false;
    }

    public static boolean isStringEmpty(String str) {
        return str == null || str.isEmpty();
    }

    public static boolean parseStringBoolean(String str) {
        if (isStringEmpty(str)) return false;
        if (str.equalsIgnoreCase("1") || str.equalsIgnoreCase("true") || str.equalsIgnoreCase("on"))
            return true;
        return false;
    }

    public static String replaceNotValidateChar(String s) {
        final String SpaceReplace = "_";
        final String Space = "[^A-Za-z0-9\\-_./]";
        return s.replaceAll(Space, SpaceReplace);
    }

    public static String trimBlank(String str) {
        if (str == null) {
            return null;
        }
        String dest = "";
        Pattern p = Pattern.compile("\\s*|\t|\r|\n");
        Matcher m = p.matcher(str);
        dest = m.replaceAll("");
        return dest;
    }

    public static String join(Collection<String> datas, char fieldSeparator) {
        StringBuffer buffer = new StringBuffer();
        for (String data : datas) {
            buffer.append(data);
            buffer.append(fieldSeparator);
        }
        if (buffer.length() > 0) {
            buffer.deleteCharAt(buffer.length() - 1);
        }
        return buffer.toString();
    }

    public static List<String> parseStrings(String s, String separator) {
        if (isStringEmpty(s)) {
            return new ArrayList<>();
        }
        String[] stringArray = s.split(separator);
        return new ArrayList<>(Arrays.asList(stringArray));
    }

    /**
     * Make a string representation of the exception.
     * @param e The exception to stringify
     * @return A string with exception name and call stack.
     */
    public static String stringifyException(Throwable e) {
        StringWriter stm = new StringWriter();
        PrintWriter wrt = new PrintWriter(stm);
        e.printStackTrace(wrt);
        wrt.close();
        return stm.toString();
    }

    /**
     *
     * @param s cannot be null, and need to is valid to be split, so can be empty
     * @param firstSeparator cannot be null or empty
     * @param secondSeparator cannot be null or empty
     * @return
     */
    public static Map<String, String> parseMapFromString(String s, String firstSeparator, String secondSeparator) {
        if (s == null || isStringEmpty(firstSeparator) || isStringEmpty(secondSeparator)) {
            return null;
        }
        List<String> attributes = StringUtils.parseStrings(s.trim(), firstSeparator);
        Map<String, String> map = new HashMap<>(attributes.size());
        for (String attribute : attributes) {
            List<String> kvs = StringUtils.parseStrings(attribute, secondSeparator);
            if (kvs.size() != 2) {
                return null;
            }
            String k = kvs.get(0).trim();
            String v = kvs.get(1).trim();
            if (StringUtils.isStringEmpty(k) || StringUtils.isStringEmpty(v)) {
                return null;
            }
            map.put(k, v);
        }
        return map;
    }

    public static short parseShort(String value, short defaultValue){
        short ret = defaultValue;
        try {
            ret = Short.valueOf(value);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public static long parseLong(String value, long defaultValue){
        long ret = defaultValue;
        try {
            ret = Long.valueOf(value);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return ret;
    }
}
