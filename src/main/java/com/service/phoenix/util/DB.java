package com.service.phoenix.util;

import com.service.phoenix.service.DataSourceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@Component("DB")
public final class DB {
    private final static Logger logger = LoggerFactory.getLogger(DataSourceService.class);

    private static String databaseUri;
    private static String driver;

    private static DB instance = null;

    private DB() {
    }

    @Value("${phoenix.url}")
    public void setDatabaseUri(String databaseUri) {
        DB.databaseUri = databaseUri;
    }

    @Value("${phoenix.driver}")
    public void setDriver(String driver) {
        DB.driver = driver;
    }

    public static DB getInstance() {
        if (instance == null) {
            synchronized (DB.class) {
                if (instance == null) {
                    instance = new DB();
                }
            }
        }
        return instance;
    }

    public Connection getConnection() {
        try {
            Class.forName(driver);
            Properties properties = new Properties();
            properties.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");
            properties.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
            return DriverManager.getConnection(databaseUri, properties);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("Connection fail: ", e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
