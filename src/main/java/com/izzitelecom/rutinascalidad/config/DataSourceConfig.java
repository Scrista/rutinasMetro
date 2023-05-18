package com.izzitelecom.rutinascalidad.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import javax.websocket.RemoteEndpoint;
import java.sql.Connection;
import java.sql.SQLException;


public class DataSourceConfig {

    private static HikariDataSource dataSourcemetro;


    static {
        try{
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setDriverClassName("oracle.jdbc.driver.OracleDriver");
            hikariConfig.setJdbcUrl("jdbc:oracle:thin:@vayu.izzitelecom.net:1594/antaresservice");
            hikariConfig.setUsername("CALIDAD");
            hikariConfig.setPassword("Calidad#2015");

            hikariConfig.setMaximumPoolSize(2);


            hikariConfig.setConnectionTimeout(900000);
            hikariConfig.setMaxLifetime(900000);

            hikariConfig.setPoolName("metro");

            hikariConfig.addDataSourceProperty("dataSource.cachePrepStmts", "true");
            hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSize", "250");
            hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSqlLimit", "2048");
            hikariConfig.addDataSourceProperty("dataSource.useServerPrepStmts", "true");

            dataSourcemetro = new HikariDataSource(hikariConfig);

            System.out.println("=================== CONECTADO A metro ===================");
        }catch (Exception e){
           e.printStackTrace();
        }
    }

    public  static DataSource metroDataSource(){
        return dataSourcemetro;
    }

}
