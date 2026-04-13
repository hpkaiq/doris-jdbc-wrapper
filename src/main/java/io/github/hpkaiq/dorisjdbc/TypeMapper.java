package io.github.hpkaiq.dorisjdbc;

import java.sql.Types;
import java.util.Locale;
import java.util.regex.*;

public class TypeMapper {
    private static final Pattern TYPE_PATTERN = Pattern.compile("([a-zA-Z0-9]+)(\\((\\d+)(,(\\d+))?\\))?");

    public static int toJdbcType(String dorisType) {
        String t = dorisType.toLowerCase(Locale.ROOT);
        if (t.startsWith("char")) return Types.CHAR;
        if (t.startsWith("varchar")) return Types.VARCHAR;
        if (t.startsWith("string")) return Types.VARCHAR;
        if (t.startsWith("boolean")) return Types.BOOLEAN;
        if (t.startsWith("tinyint")) return Types.TINYINT;
        if (t.startsWith("smallint")) return Types.SMALLINT;
        if (t.startsWith("int")) return Types.INTEGER;
        if (t.startsWith("bigint")) return Types.BIGINT;
        if (t.startsWith("float")) return Types.FLOAT;
        if (t.startsWith("double")) return Types.DOUBLE;
        if (t.startsWith("decimal")) return Types.DECIMAL;
        if (t.startsWith("datetime")) return Types.TIMESTAMP;
        if (t.startsWith("date")) return Types.DATE;
        if (t.startsWith("text")) return Types.LONGVARCHAR;
        if (t.startsWith("array")) return Types.ARRAY;
        if (t.startsWith("json")) return Types.STRUCT;
        return Types.OTHER;
    }

    public static int extractLength(String dorisType) {
        String t = dorisType.replaceAll("\\(.*\\)", "").toLowerCase();
        if (t.startsWith("tinyint")) return 3;
        if (t.startsWith("smallint")) return 5;
        if (t.startsWith("int")) return 10;
        if (t.startsWith("bigint")) return 19;
        if (t.startsWith("datetime")) return 19;
        if (t.startsWith("date")) return  10;
        if (t.startsWith("boolean")) return  1;
        if (t.startsWith("json") || t.startsWith("text")) return 1048576;

        Matcher m = TYPE_PATTERN.matcher(dorisType);
        if (m.find() && m.group(3) != null) {
            return Integer.parseInt(m.group(3));
        }
        return 0;
    }

    public static int extractScale(String dorisType) {
        Matcher m = TYPE_PATTERN.matcher(dorisType);
        if (m.find() && m.group(5) != null) {
            return Integer.parseInt(m.group(5));
        }
        return 0;
    }

    public static String baseTypeName(String dorisType) {
        if (dorisType == null) return null;
        return dorisType.replaceAll("\\(.*?\\)", "")
                .replaceAll("v\\d+$", "");
    }

    public static boolean isCharType(String dorisType) {
        if (dorisType == null) return false;
        String t = dorisType.toLowerCase(Locale.ROOT);
        return t.startsWith("char") || t.startsWith("varchar") || t.startsWith("string") || t.startsWith("text");
    }

    public static int extractNullAble(String nullabilityInfo) {
        if (nullabilityInfo.equals("YES")) {
            return java.sql.DatabaseMetaData.columnNullable;
        } else if (nullabilityInfo.equals("UNKNOWN")) {
            return java.sql.DatabaseMetaData.columnNullableUnknown;
        } else {
            return java.sql.DatabaseMetaData.columnNoNulls;
        }
    }
}