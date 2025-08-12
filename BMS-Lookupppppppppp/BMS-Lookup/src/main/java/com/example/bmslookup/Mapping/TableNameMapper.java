package com.example.bmslookup.Mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;

public class TableNameMapper {
    private static final Map<String, String> TABLE_NAME_MAPPING = new HashMap<>();

  static {
    TABLE_NAME_MAPPING.put("Area", "GDEV1T_UHI_DATA.bms_Area_lkp");
    TABLE_NAME_MAPPING.put("Country", "GDEV1T_UHI_DATA.bms_Country_lkp");
    TABLE_NAME_MAPPING.put("City", "GDEV1T_UHI_DATA.bms_City_lkp");
    TABLE_NAME_MAPPING.put("MossCategory", "GDEV1T_UHI_DATA.bms_MossCategory_lkp");
    TABLE_NAME_MAPPING.put("DiseaseList", "GDEV1T_UHI_DATA.bms_DiseaseList_lkp");
    TABLE_NAME_MAPPING.put("MossIndicator", "GDEV1T_UHI_DATA.bms_MossIndicator_lkp");
    TABLE_NAME_MAPPING.put("SocialStatus", "GDEV1T_UHI_DATA.bms_SocialStatus_lkp");
    TABLE_NAME_MAPPING.put("CollectionEntities", "GDEV1T_UHI_DATA.bms_CollectionEntities_lkp");
    TABLE_NAME_MAPPING.put("Language", "GDEV1T_UHI_DATA.bms_Language_lkp");
    TABLE_NAME_MAPPING.put("Nationality", "GDEV1T_UHI_DATA.bms_Nationality_lkp");
    TABLE_NAME_MAPPING.put("SickCategory", "GDEV1T_UHI_DATA.bms_SickCategory_lkp");
    TABLE_NAME_MAPPING.put("DeactivationReasons", "GDEV1T_UHI_DATA.bms_DeactivationReasons_lkp");
    TABLE_NAME_MAPPING.put("Governorate", "GDEV1T_UHI_DATA.bms_Governorate_lkp");
    TABLE_NAME_MAPPING.put("Education", "GDEV1T_UHI_DATA.bms_Education_lkp");
    TABLE_NAME_MAPPING.put("MossSubCategory", "GDEV1T_UHI_DATA.bms_MossSubCategory_lkp");
    TABLE_NAME_MAPPING.put("Provider", "GDEV1T_UHI_DATA.bms_Provider_lkp");
    TABLE_NAME_MAPPING.put("Gender", "GDEV1T_UHI_DATA.bms_Gender_lkp");
    TABLE_NAME_MAPPING.put("MilitaryService", "GDEV1T_UHI_DATA.bms_MilitaryService_lkp");
    TABLE_NAME_MAPPING.put("Telecom", "GDEV1T_UHI_DATA.bms_Telecom_lkp");
    TABLE_NAME_MAPPING.put("Relation", "GDEV1T_UHI_DATA.bms_Relation_lkp");
    TABLE_NAME_MAPPING.put("Cluster", "GDEV1T_UHI_DATA.bms_Cluster_lkp");

    // Beneficiary and related tables
    TABLE_NAME_MAPPING.put("beneficiary", "GDEV1T_UHI_DATA.beneficiary");
    TABLE_NAME_MAPPING.put("contact", "GDEV1T_UHI_DATA.contact");
    TABLE_NAME_MAPPING.put("employment", "GDEV1T_UHI_DATA.employment");
}


    public static String resolve(String alias) {
        return TABLE_NAME_MAPPING.getOrDefault(alias, alias);
    }
}