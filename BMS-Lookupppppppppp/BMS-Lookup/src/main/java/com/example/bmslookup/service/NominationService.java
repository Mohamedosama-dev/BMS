package com.example.bmslookup.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class NominationService {

    private static final Logger logger = LoggerFactory.getLogger(NominationService.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Deactivates the old Head of Family.
     * Finds the beneficiary with the given familyId and 'Head of Family' relation and sets their activationStatus to 'inactive'.
     * @param familyId The family ID.
     * @return The number of rows affected.
     */
    public int deactivateOldHeadOfFamily(String familyId) {
        if (familyId == null || familyId.trim().isEmpty()) {  // Check if familyId is null or empty
            logger.warn("deactivateOldHeadOfFamily called with null or empty familyId. No action taken.");
            return 0;
        }
        String sql = "UPDATE GDEV1T_UHI_DATA.beneficiary SET \"activationStatus\" = 'inactive' WHERE \"familyId\" = ? AND LOWER(\"familyRelation\") = LOWER('Head of Family')"; // Update the activationStatus to 'inactive' for the Head of Family
        int rowsAffected = jdbcTemplate.update(sql, familyId); // Execute the update query with the provided familyId
        logger.info("Deactivated old Head of Family for familyId {}. Rows affected: {}", familyId, rowsAffected);
        return rowsAffected;
    }
    
    /**
     * Updates familyId for all dependents (non-Head of Family members) from oldFamilyId to newFamilyId.
     * Does NOT change their activationStatus.
     * @param oldFamilyId The current family ID.
     * @param newFamilyId The new family ID.
     * @return The number of rows affected.
     */
    public int updateFamilyIdForDependents(String oldFamilyId, String newFamilyId) {
        if (oldFamilyId == null || oldFamilyId.trim().isEmpty() || newFamilyId == null || newFamilyId.trim().isEmpty()) {
            logger.warn("updateFamilyIdForDependents called with null or empty IDs. No action taken.");
            return 0;
        }

        String sql = "UPDATE GDEV1T_UHI_DATA.beneficiary SET \"familyId\" = ? WHERE \"familyId\" = ? AND LOWER(\"familyRelation\") <> LOWER('Head of Family')";
        
        try {
            logger.info("Updating familyId for dependents from {} to {}.", oldFamilyId, newFamilyId);
            int rowsAffected = jdbcTemplate.update(sql, newFamilyId, oldFamilyId);
            logger.info("Successfully updated familyId for {} dependents from {} to {}.", rowsAffected, oldFamilyId, newFamilyId);
            return rowsAffected;
        } catch (Exception e) {
            logger.error("Failed to update familyId for dependents of familyId: {}", oldFamilyId, e);
            throw new RuntimeException("Failed to update familyId for dependents", e);
        }
    }

    /**
     * Performs the full nomination process within a single database transaction.
     * This ensures that if any step fails, the entire operation is rolled back.
     * Steps:
     * 1. Deactivate the old Head of Family (set activationStatus to 'inactive')
     * 2. Update all dependents to have the new familyId (without changing their activationStatus)
     * 3. Insert the new Head of Family record with the new familyId
     */
    /**
     * Updates familyRelation to "Head of Family" for the record with the given ID.
     * @param id The beneficiary ID.
     * @return The number of rows affected.
     */
    public int updateFamilyRelationForNewFamilyId(String id) {
        if (id == null || id.trim().isEmpty()) {
            logger.warn("updateFamilyRelationForNewFamilyId called with null or empty id. No action taken.");
            return 0;
        }
        
        // First, check if a record with this ID exists
        String checkSql = "SELECT COUNT(*) FROM GDEV1T_UHI_DATA.beneficiary WHERE Id = ?";
        Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, id);
        
        if (count == null || count == 0) {
            logger.warn("No beneficiary found with id {}. No update performed.", id);
            return 0;
        }

        // If the record exists, update its familyRelation
        String updateSql = "UPDATE GDEV1T_UHI_DATA.beneficiary SET familyRelation = 'Head of Family' WHERE Id = ?";
        int rowsAffected = jdbcTemplate.update(updateSql, id);
        logger.info("Updated familyRelation to Head of Family for beneficiary with id {}. Rows affected: {}", id, rowsAffected);
        return rowsAffected;
    }

    @Transactional
    public void processNomination(String oldFamilyId, String newFamilyId, String newHofId, String createdBy) {
        logger.info("Starting transactional nomination process for familyId: {}", oldFamilyId);
        
        try {
            // Step 1: Deactivate old Head of Family
            deactivateOldHeadOfFamily(oldFamilyId);
            
            // Step 2: Update dependents to new familyId
            updateFamilyIdForDependents(oldFamilyId, newFamilyId);
            
            // Step 3: Set the new Head of Family's relation
            updateFamilyRelationForNewFamilyId(newHofId);
            
            logger.info("Nomination process completed successfully. Old familyId: {}, New familyId: {}", oldFamilyId, newFamilyId);
            
        } catch (Exception e) {
            logger.error("Nomination process failed for familyId: {}", oldFamilyId, e);
            throw new RuntimeException("Nomination process failed", e);
        }
    }
}
