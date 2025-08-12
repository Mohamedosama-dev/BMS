package com.example.bmslookup.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Teradata database configuration.
 * Configures DataSource, JdbcTemplate, and TransactionManager.
 */
@Configuration
@EnableTransactionManagement
public class JdbcConfig {

    private static final Logger logger = LoggerFactory.getLogger(JdbcConfig.class);

    @Value("${spring.datasource.url}")
    private String databaseUrl;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    @Value("${spring.datasource.hikari.maximum-pool-size:10}")
    private int maximumPoolSize;

    @Value("${spring.datasource.hikari.minimum-idle:5}")
    private int minimumIdle;

    @Value("${spring.datasource.hikari.idle-timeout:300000}")
    private long idleTimeout;

    @Value("${spring.datasource.hikari.connection-timeout:20000}")
    private long connectionTimeout;

    @Value("${spring.datasource.hikari.max-lifetime:1200000}")
    private long maxLifetime;

    @Value("${spring.datasource.hikari.leak-detection-threshold:60000}")
    private long leakDetectionThreshold;

    /**
     * Configures the HikariCP DataSource for Teradata.
     * 
     * @return configured DataSource
     */
    @Bean
    public DataSource dataSource() {
        logger.info("Configuring DataSource for Teradata");

        HikariConfig config = new HikariConfig();

        // Basic connection settings
        config.setJdbcUrl(databaseUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName("com.teradata.jdbc.TeraDriver");

        // Pool settings
        config.setMaximumPoolSize(maximumPoolSize);
        config.setMinimumIdle(minimumIdle);
        config.setIdleTimeout(idleTimeout);
        config.setConnectionTimeout(connectionTimeout);
        config.setMaxLifetime(maxLifetime);
        config.setLeakDetectionThreshold(leakDetectionThreshold);

        // Teradata-specific settings
        config.addDataSourceProperty("TMODE", "TERA");
        config.addDataSourceProperty("CHARSET", "UTF8");
        config.addDataSourceProperty("LOGMECH", "LDAP");
        config.addDataSourceProperty("DBS_PORT", "1025");
        config.addDataSourceProperty("SESSIONS", "1");
        config.addDataSourceProperty("MAX_SESSIONS", "10");
        config.addDataSourceProperty("SESSIONS_PER_USER", "5");
        config.addDataSourceProperty("MAX_SESSIONS_PER_USER", "10");

        // Performance settings
        config.addDataSourceProperty("QUERY_BAND", "BMS_LOOKUP_SERVICE");
        config.addDataSourceProperty("QUERY_TIMEOUT", "300");
        config.addDataSourceProperty("LOGIN_TIMEOUT", "30");
        config.addDataSourceProperty("SOCKET_TIMEOUT", "300");

        // Security settings
        config.addDataSourceProperty("ENCRYPT_DATA", "true");
        config.addDataSourceProperty("ENCRYPT_METHOD", "SSL");
        config.addDataSourceProperty("TRUSTED_CERTIFICATE", "true");

        // Connection pool settings
        config.setPoolName("BMSLookupHikariPool");
        config.setRegisterMbeans(true);
        config.setAllowPoolSuspension(false);
        config.setAutoCommit(true);
        config.setReadOnly(false);
        config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");

        // Validation settings
        config.setConnectionTestQuery("SELECT 1");
        config.setValidationTimeout(5000);

        HikariDataSource dataSource = new HikariDataSource(config);

        // Test connection
        testConnection(dataSource);

        logger.info("DataSource configured successfully");
        return dataSource;
    }

    /**
     * Configures JdbcTemplate.
     * 
     * @param dataSource configured DataSource
     * @return configured JdbcTemplate
     */
    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        logger.info("Configuring JdbcTemplate");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        // JdbcTemplate settings
        jdbcTemplate.setFetchSize(100);
        jdbcTemplate.setMaxRows(1000);
        jdbcTemplate.setQueryTimeout(300);
        jdbcTemplate.setSkipResultsProcessing(false);
        jdbcTemplate.setSkipUndeclaredResults(false);
        jdbcTemplate.setResultsMapCaseInsensitive(true);

        logger.info("JdbcTemplate configured successfully");
        return jdbcTemplate;
    }

    /**
     * Configures TransactionManager.
     * 
     * @param dataSource configured DataSource
     * @return configured PlatformTransactionManager
     */
    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        logger.info("Configuring TransactionManager");

        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);

        // Transaction settings
        transactionManager.setDefaultTimeout(30); // 30 seconds
        transactionManager.setRollbackOnCommitFailure(true);
        transactionManager.setEnforceReadOnly(false);

        logger.info("TransactionManager configured successfully");
        return transactionManager;
    }

    /**
     * Tests database connection.
     * 
     * @param dataSource DataSource to test
     */
    private void testConnection(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            logger.info("Database connection test succeeded");
            logger.info("Database URL: {}", databaseUrl);
            logger.info("Database Product: {}", connection.getMetaData().getDatabaseProductName());
            logger.info("Database Version: {}", connection.getMetaData().getDatabaseProductVersion());
            logger.info("Driver Name: {}", connection.getMetaData().getDriverName());
            logger.info("Driver Version: {}", connection.getMetaData().getDriverVersion());
        } catch (SQLException e) {
            logger.error("Database connection test failed", e);
            throw new RuntimeException("Failed to connect to the database", e);
        }
    }

    /**
     * Monitors connection pool state.
     * 
     * @param dataSource DataSource to monitor
     */
    public void monitorConnectionPool(DataSource dataSource) {
        if (dataSource instanceof HikariDataSource) {
            HikariDataSource hikariDataSource = (HikariDataSource) dataSource;

            logger.info("=== Connection Pool Status ===");
            logger.info("Active Connections: {}", hikariDataSource.getHikariPoolMXBean().getActiveConnections());
            logger.info("Idle Connections: {}", hikariDataSource.getHikariPoolMXBean().getIdleConnections());
            logger.info("Total Connections: {}", hikariDataSource.getHikariPoolMXBean().getTotalConnections());
            logger.info("Threads Awaiting Connection: {}", hikariDataSource.getHikariPoolMXBean().getThreadsAwaitingConnection());
        }
    }

    /**
     * Safely closes the DataSource.
     * 
     * @param dataSource DataSource to close
     */
    public void closeDataSource(DataSource dataSource) {
        if (dataSource instanceof HikariDataSource) {
            HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
            hikariDataSource.close();
            logger.info("DataSource closed successfully");
        }
    }
}
