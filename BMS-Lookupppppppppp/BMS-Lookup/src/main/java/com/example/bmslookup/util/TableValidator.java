package com.example.bmslookup.util;

import com.example.bmslookup.Mapping.TableNameMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Utility class for validating table names and preventing SQL injection.
 */
@Component
public class TableValidator {

    private static final Logger logger = LoggerFactory.getLogger(TableValidator.class);

    // Map of allowed table aliases to full table names (TableNameMapper is used instead of this map)
    private static final Map<String, String> ALLOWED_TABLE_MAPPINGS = new HashMap<>();

    static {
        // This list is used to validate full table names only
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_Area_lkp", "GDEV1T_UHI_DATA.bms_Area_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_Country_lkp", "GDEV1T_UHI_DATA.bms_Country_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_City_lkp", "GDEV1T_UHI_DATA.bms_City_lkp");

        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_MossCategory_lkp", "GDEV1T_UHI_DATA.bms_MossCategory_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_DiseaseList_lkp", "GDEV1T_UHI_DATA.bms_DiseaseList_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_MossIndicator_lkp", "GDEV1T_UHI_DATA.bms_MossIndicator_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_SocialStatus_lkp", "GDEV1T_UHI_DATA.bms_SocialStatus_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_CollectionEntities_lkp", "GDEV1T_UHI_DATA.bms_CollectionEntities_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_Language_lkp", "GDEV1T_UHI_DATA.bms_Language_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_Nationality_lkp", "GDEV1T_UHI_DATA.bms_Nationality_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_SickCategory_lkp", "GDEV1T_UHI_DATA.bms_SickCategory_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_DeactivationReasons_lkp", "GDEV1T_UHI_DATA.bms_DeactivationReasons_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_Governorate_lkp", "GDEV1T_UHI_DATA.bms_Governorate_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_Education_lkp", "GDEV1T_UHI_DATA.bms_Education_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_MossSubCategory_lkp", "GDEV1T_UHI_DATA.bms_MossSubCategory_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_Provider_lkp", "GDEV1T_UHI_DATA.bms_Provider_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_Gender_lkp", "GDEV1T_UHI_DATA.bms_Gender_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_MilitaryService_lkp", "GDEV1T_UHI_DATA.bms_MilitaryService_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_Telecom_lkp", "GDEV1T_UHI_DATA.bms_Telecom_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_Relation_lkp", "GDEV1T_UHI_DATA.bms_Relation_lkp");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.bms_Cluster_lkp", "GDEV1T_UHI_DATA.bms_Cluster_lkp");
        
        // Beneficiary and related tables
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.beneficiary", "GDEV1T_UHI_DATA.beneficiary");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.contact", "GDEV1T_UHI_DATA.contact");
        ALLOWED_TABLE_MAPPINGS.put("GDEV1T_UHI_DATA.employment", "GDEV1T_UHI_DATA.employment");
    }

    // Pattern for valid table name format
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile(
        "^[A-Za-z0-9_]+(\\.[A-Za-z0-9_]+)?$"
    );

    // Reserved SQL keywords to check against
    private static final Set<String> SQL_KEYWORDS = new HashSet<>(Arrays.asList(
        "SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
        "EXEC", "EXECUTE", "UNION", "JOIN", "WHERE", "FROM", "INTO", "VALUES",
        "SET", "AND", "OR", "NOT", "LIKE", "IN", "BETWEEN", "ORDER", "GROUP",
        "HAVING", "DISTINCT", "TOP", "LIMIT", "OFFSET", "CASE", "WHEN", "THEN",
        "ELSE", "END", "IF", "EXISTS", "ALL", "ANY", "SOME", "NULL", "TRUE", "FALSE"
    ));

    /**
     * Checks if the provided table name is allowed and safe.
     *
     * @param tableName The table name to validate
     * @return true if the table name is valid and allowed
     */
    public boolean isValidTable(String tableName) {
        String resolvedTableName = resolveTableName(tableName);

        if (resolvedTableName == null || resolvedTableName.trim().isEmpty()) {
            logger.warn("Table name is empty or unknown: {}", tableName);
            return false;
        }

        if (!TABLE_NAME_PATTERN.matcher(resolvedTableName).matches()) {
            logger.warn("Table name does not match the allowed pattern: {}", resolvedTableName);
            return false;
        }

        if (containsSqlKeywords(resolvedTableName)) {
            logger.warn("Table name contains SQL reserved keywords: {}", resolvedTableName);
            return false;
        }

        if (containsDangerousCharacters(resolvedTableName)) {
            logger.warn("Table name contains dangerous characters: {}", resolvedTableName);
            return false;
        }

        logger.debug("Valid table name: {}", resolvedTableName);
        return true;
    }

    public String resolveTableName(String aliasOrFullName) {
        if (aliasOrFullName == null) return null;

        String trimmed = aliasOrFullName.trim();

        // Use TableNameMapper to resolve aliases
        String resolved = TableNameMapper.resolve(trimmed);
        if (resolved != null && !resolved.equals(trimmed)) {
            return resolved;
        }

        // If full name exists in the allowed list
        if (ALLOWED_TABLE_MAPPINGS.containsKey(trimmed)) {
            return trimmed;
        }

        return null;
    }

    private boolean containsSqlKeywords(String tableName) {
        String upperTableName = tableName.toUpperCase();
        for (String keyword : SQL_KEYWORDS) {
            if (upperTableName.contains(keyword)) {
                return true;
            }
        }
        return false;
    }

    private boolean containsDangerousCharacters(String tableName) {
        String dangerousChars = "';\"\\-/*(){}[]|&^%$#@!~`+=<>?";
        for (char c : dangerousChars.toCharArray()) {
            if (tableName.indexOf(c) != -1) {
                return true;
            }
        }
        return false;
    }

    public String sanitizeTableName(String tableName) {
        if (tableName == null) {
            return null;
        }

        String sanitized = tableName.trim();
        sanitized = sanitized.replaceAll("[^A-Za-z0-9_.]", "");
        sanitized = sanitized.replaceAll("^\\.|\\.$", "");

        return sanitized;
    }

    public Set<String> getAllowedTableAliases() {
        return new HashSet<>(ALLOWED_TABLE_MAPPINGS.keySet());
    }

    public boolean addAllowedTable(String alias, String fullTableName) {
        if (alias == null || fullTableName == null) {
            return false;
        }

        alias = alias.trim();
        fullTableName = fullTableName.trim();

        if (!TABLE_NAME_PATTERN.matcher(fullTableName).matches() || containsDangerousCharacters(fullTableName)) {
            return false;
        }

        ALLOWED_TABLE_MAPPINGS.put(alias, fullTableName);
        logger.info("Table added: {} -> {}", alias, fullTableName);
        return true;
    }

    public boolean removeAllowedTable(String alias) {
        if (alias == null) {
            return false;
        }

        String removed = ALLOWED_TABLE_MAPPINGS.remove(alias.trim());
        if (removed != null) {
            logger.info("Table removed: {}", alias);
            return true;
        }
        return false;
    }

    public boolean startsWithPrefix(String tableName, String prefix) {
        if (tableName == null || prefix == null) {
            return false;
        }

        return tableName.trim().toLowerCase().startsWith(prefix.toLowerCase());
    }

    public boolean endsWithSuffix(String tableName, String suffix) {
        if (tableName == null || suffix == null) {
            return false;
        }

        return tableName.trim().toLowerCase().endsWith(suffix.toLowerCase());
    }
}
