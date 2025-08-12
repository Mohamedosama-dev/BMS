package com.example.bmslookup.test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@Component
public class DatabaseTest implements CommandLineRunner {

    @Autowired
    private DataSource dataSource;

    @Override
    public void run(String... args) throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            String sql = "SELECT COUNT(*) AS total FROM GDEV1T_UHI_DATA.bms_Country_lkp";
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                System.out.println("Total rows in bms_Country_lkp: " + rs.getInt("total"));
            }
        } catch (Exception e) {
            System.out.println(" Error connecting to DB: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
