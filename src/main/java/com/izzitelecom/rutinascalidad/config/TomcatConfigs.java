package com.izzitelecom.rutinascalidad.config;
//import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import javafx.application.Application;
import org.apache.catalina.Context;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.descriptor.web.ContextResource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.embedded.tomcat.*;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.jndi.JndiObjectFactoryBean;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class TomcatConfigs  {

    @Bean
    public TomcatEmbeddedServletContainerFactory tomcatFactory() {

        return new TomcatEmbeddedServletContainerFactory() {

            @Override
            protected TomcatEmbeddedServletContainer getTomcatEmbeddedServletContainer(Tomcat tomcat)
            {
                tomcat.enableNaming();
                return super.getTomcatEmbeddedServletContainer(tomcat);
            }

            @Override
            protected void postProcessContext(Context context)
            {
                ContextResource resource = new ContextResource();

                resource.setType(DataSource.class.getName());
                resource.setName("gestionop");

                resource.setProperty("factory", "org.apache.tomcat.jdbc.pool.DataSourceFactory");

                resource.setProperty("driverClassName", "oracle.jdbc.driver.OracleDriver");

                resource.setProperty("url", "jdbc:oracle:thin:@ ttjibdprdhouse.izzitelecom.net:1585:MITILO");

                resource.setProperty("username", "GOTECNICOS");

                resource.setProperty("password", "Qazse#GOTECNICOS22");

                resource.setProperty("maxActive", "40");
//                resource.setProperty("maxIdle", "30");
                resource.setProperty("maxWait", "10000");


                context.getNamingResources().addResource(resource);
            }
        };
    }


    @Bean
    public TomcatEmbeddedServletContainerFactory tomcatFactoryCalildad() {

        return new TomcatEmbeddedServletContainerFactory() {

            @Override
            protected TomcatEmbeddedServletContainer getTomcatEmbeddedServletContainer(Tomcat tomcat)
            {
                tomcat.enableNaming();
                return super.getTomcatEmbeddedServletContainer(tomcat);
            }

            @Override
            protected void postProcessContext(Context context)
            {
                ContextResource resource = new ContextResource();

                resource.setType(DataSource.class.getName());
                resource.setName("calidad");

                resource.setProperty("factory", "org.apache.tomcat.jdbc.pool.DataSourceFactory");

                resource.setProperty("driverClassName", "oracle.jdbc.driver.OracleDriver");

                resource.setProperty("url", "");

                resource.setProperty("username", "");

                resource.setProperty("password", "");

                resource.setProperty("maxActive", "1");
                resource.setProperty("initialSize", "0");
//                resource.setProperty("maxIdle", "30");
//                resource.setProperty("maxWait", "10000");


                context.getNamingResources().addResource(resource);
            }
        };
    }

    @Bean
    public DataSource jndiDataSource() throws IllegalArgumentException, NamingException
    {
        JndiObjectFactoryBean bean = new JndiObjectFactoryBean();
        bean.setJndiName("java:/comp/env/gestionop");

        bean.setProxyInterface(DataSource.class);
        bean.setLookupOnStartup(false);
        bean.afterPropertiesSet();

        return (DataSource) bean.getObject();
    }



}