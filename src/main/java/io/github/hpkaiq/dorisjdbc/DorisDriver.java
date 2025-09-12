package io.github.hpkaiq.dorisjdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public final class DorisDriver implements Driver {
    private static final String URL_PREFIX = "jdbc:doris://";
    private static final Driver MYSQL;

    static {
        try {
            MYSQL = new com.mysql.cj.jdbc.Driver();
            DriverManager.registerDriver(new DorisDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;
        String mysqlUrl = "jdbc:mysql://" + url.substring(URL_PREFIX.length());
        Connection raw = MYSQL.connect(mysqlUrl, info);
        return new DorisConnection(raw);
    }

    @Override public boolean acceptsURL(String url) { return url != null && url.startsWith(URL_PREFIX); }
    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) { return new DriverPropertyInfo[0]; }
    @Override public int getMajorVersion() { return 1; }
    @Override public int getMinorVersion() { return 0; }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public Logger getParentLogger() { return Logger.getGlobal(); }
}
