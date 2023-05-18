package com.izzitelecom.rutinascalidad.tool;

import com.izzitelecom.rutinascalidad.config.DataSourceConfig;
import com.zaxxer.hikari.HikariConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
//import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class UtilDataSource extends DataSourceConfig {
/*

    @Autowired
    @Qualifier("calidadTemplate")
    private JdbcTemplate MetroTemplate;
*/
   /* @Autowired
    @Qualifier("oraclejdbcTemplate")*/
    @Autowired
    private JdbcTemplate gestopTemplate;

/*<Resource name="" auth="Container" type="javax.sql.DataSource"
    removeAbandonedTimeout="10" minEvictableIdleTimeMillis="10000" removeAbandoned="true" initialSize="0"
    maxActive="1" maxConnLifetimeMillis="900000" maxTotal="1" maxIdle="1" maxAge="900000" username=""
    password="" driverClassName="oracle.jdbc.driver.OracleDriver"
    url="" />*/


    public Connection getExternalMETROConnectionFromPool() throws SQLException {
       DataSource opulidoDataSource = metroDataSource();
        System.out.println("=================== obtener Conexion a Metro ===================");

        return opulidoDataSource.getConnection();
    }

    public  Connection getOracleConnectionFromPool() throws SQLException {
        DataSource gestopDataSource = gestopTemplate.getDataSource();
        System.out.println("=================== obtener Conexion a GESTOP ===================");

        return gestopDataSource.getConnection();
    }
}
