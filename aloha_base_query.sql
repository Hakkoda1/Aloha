-----------------------------------------------------------------------------
---         What: Gather Aloha Variables
---         Who: Dana George, Nicole Abarca
---         When: 07/09/2022
---         Details:
---              X Length of Stay
---                Benefits/Plan type
---              X Social determinants of health
---              X Zip code/median income
---              X Age groups, gender, 
---                Home type (nursing home Y/N)
---                Transportation access
---                Snowflake Data Marketplace (Census Data)
---                Comorbidity Index
---                COPS/COPS2, Charleston, Internal
---                Frailty (Questionnaire - Can use x alone)
---              X Number of hospitalizations in last 5 years
---              X Hospital Location
---                Clinic Department - check more in detail ENC.class:code::text
---              X Day of week, Time of day, Season of year
---                Transferred from another facility Y/N
---              X Vitals - Trend throughout encounter, vital date and time
---                Clincal Status / DX Status in Condition table in Condition Resource Type
---                ConditionVerificationStatus
---                Care Management Plan look at how many of each
---                Previous visit w/in 24 hours? What condition did you have there? Why were they there?
---                Concept ID from URL - Nicole
---                Readmission vs transferred w/in 24 hours, message dale
---              X PRECOVID VS COVID
-----------------------------------------------------------------------------


SELECT DISTINCT
  PAT.ID AS PAT_ID
, ENC.ID AS ENC_ID
, CAST(DATEDIFF(DAY, PAT.BIRTHDATE, GETDATE()) / (365.23076923074) AS INT) AS PAT_AGE
, PAT.GENDER
, ZIP.POSTALCODE AS PAT_ZIP
, ENC.class:code::text as ENC_TYPE
, TO_DATE(LEFT(PER.PERIOD_START, 10), 'YYYY-MM-DD') AS PERIOD_START
, TO_DATE(LEFT(PER.PERIOD_END, 10), 'YYYY-MM-DD') AS PERIOD_END
, dayname(TO_DATE(LEFT(PER.PERIOD_START, 10), 'YYYY-MM-DD')) AS PERIOD_START_DAY
, dayname(TO_DATE(LEFT(PER.PERIOD_END, 10), 'YYYY-MM-DD')) AS PERIOD_END_DAY
--, TO_TIME(RIGHT(TRIM(PER.PERIOD_START, '.000'), 8), 'HH:MI:SS') AS PERIOD_START_TIME                          -- REMOVE DATE, REMOVE TRAIL, REMOVE SPACES AND ANYTHING TO LEFT
, RIGHT(TRIM(PER.PERIOD_START, '.000'), 8) AS PERIOD_START_TIME                                                 -- TODO: REMOVE SPACES AND LEADING NUMS
, RIGHT(TRIM(PER.PERIOD_END, '.000'), 8) AS PERIOD_END_TIME
, (CASE WHEN MONTH(PER.PERIOD_START) IN (12, 1, 2) THEN 'WINTER'
      WHEN MONTH(PER.PERIOD_START) IN (3, 4, 5) THEN 'SPRING'
      WHEN MONTH(PER.PERIOD_START) IN (6, 7, 8) THEN 'SUMMER'
      WHEN MONTH(PER.PERIOD_START) IN (9, 10, 11) THEN 'FALL' END) AS PERIOD_START_SEASON
, (CASE WHEN MONTH(PER.PERIOD_START) IN (12, 1, 2) THEN 'WINTER'
      WHEN MONTH(PER.PERIOD_END) IN (3, 4, 5) THEN 'SPRING'
      WHEN MONTH(PER.PERIOD_END) IN (6, 7, 8) THEN 'SUMMER'
      WHEN MONTH(PER.PERIOD_END) IN (9, 10, 11) THEN 'FALL' END) AS PERIOD_END_SEASON
, TO_DATE(LEFT(PER.PERIOD_END, 10), 'YYYY-MM-DD') - TO_DATE(LEFT(PER.PERIOD_START, 10), 'YYYY-MM-DD') AS LOS
, LOC.location:display::varchar as HOSPITAL
, COUNT(MED.medicationcodeableconcept:text::text) OVER(PARTITION BY ENC.ID) AS NUM_MEDS
, COUNT(PROC.code:text::text) OVER(PARTITION BY ENC.ID) as NUM_PROCS
, VIT.code:text::text AS VITAL_SIGN
, VIT.valuequantity:unit::text AS VITAL_UNIT
, VIT.valuequantity:value AS VITAL_MEASURE
, TO_DATE(LEFT(VIT.EFFECTIVEDATETIME, 10), 'YYYY-MM-DD') AS VITAL_DATE
, TO_TIME(LEFT(SUBSTRING(VIT.EFFECTIVEDATETIME, 12, len(VIT.EFFECTIVEDATETIME)), 8), 'HH24:MI:SS') AS VITAL_TIME
, COUNT(HOSP.PARENT_ID) OVER(PARTITION BY ENC.ID) AS NUM_HOSP_LST5YRS
, CASE WHEN TO_DATE(LEFT(PERIOD_START, 10), 'YYYY-MM-DD') < '2020-03-01' THEN 'PRECOVID' ELSE 'COVID' END AS COVID

FROM "HEALTHCARE"."FHIR_FLATTENED"."FHIR_PATIENT" PAT
LEFT JOIN "HEALTHCARE"."FHIR_FLATTENED"."FHIR_ENCOUNTER" ENC ON ENC.subject:reference::text = CONCAT('urn:uuid:', PAT.ID)     -- GET ENC DATA
LEFT JOIN "HEALTHCARE"."FHIR_FLATTENED"."FHIR_ENCOUNTER_HOSPITALIZATION" HOSP ON HOSP.PARENT_ID = ENC.ID                      -- ONLY HOSPITALIZED ENCOUNTERS
LEFT JOIN "HEALTHCARE"."FHIR_FLATTENED"."FHIR_PERIOD" PER ON PER.PARENT_ID = ENC.ID
LEFT JOIN "HEALTHCARE"."FHIR_FLATTENED"."FHIR_ADDRESS" ZIP ON ZIP.PARENT_ID = PAT.ID
LEFT JOIN "HEALTHCARE"."FHIR_FLATTENED"."FHIR_ENCOUNTER_LOCATION" LOC ON LOC.PARENT_ID = ENC.ID
LEFT JOIN "HEALTHCARE"."FHIR_FLATTENED"."FHIR_MEDICATIONREQUEST" MED ON MED.encounter:reference::text = CONCAT('urn:uuid:', ENC.ID) -- MEDICATIONS ORDERED DURING ENCOUNTER
LEFT JOIN "HEALTHCARE"."FHIR_FLATTENED"."FHIR_PROCEDURE" PROC ON PROC.encounter:reference::text = CONCAT('urn:uuid:', ENC.ID)
--LEFT JOIN "HEALTHCARE"."FHIR_FLATTENED"."FHIR_CAREPLAN" CAREP ON CAREP.encounter:reference::text = CONCAT('urn:uuid:', ENC.ID)
LEFT JOIN "HEALTHCARE"."FHIR_FLATTENED"."FHIR_OBSERVATION" VIT ON VIT.encounter:reference::text = CONCAT('urn:uuid:', ENC.ID)
WHERE PAT.DECEASEDDATETIME IS NULL                                                                                            -- ONLY LIVING
AND CAST(DATEDIFF(DAY, PAT.BIRTHDATE, GETDATE()) / (365.23076923074) AS INT) >= 18                                            -- ONLY 18+
AND ZIP.PARENT_RESOURCE = 'FHIR_PATIENT'                                                                                      -- ONLY PATIENT PARENT IDS
AND PER.PARENT_RESOURCE = 'FHIR_ENCOUNTER'
AND LOC.PARENT_RESOURCE = 'FHIR_ENCOUNTER'
AND TO_DATE(LEFT(PER.PERIOD_START, 10), 'YYYY-MM-DD') >= '2018-01-01'
AND TO_DATE(LEFT(PER.PERIOD_END, 10), 'YYYY-MM-DD') <= '2023-12-31'
AND ENC.class:code::text = 'IMP'                                                                                              -- INPATIENT HOSPITALIZATIONS ONLY

LIMIT 1000;