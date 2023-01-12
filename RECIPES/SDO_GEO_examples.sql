--Get the Geometry Column name of a Spatial table
 SELECT column_name GEOM_NAME
 FROM  ALL_SDO_GEOM_METADATA
 WHERE owner = 'WHSE_FOREST_VEGETATION'
    AND table_name = 'RSLT_OPENING_SVW';

--Get the SRID of a Spatial table
SELECT s.GEOMETRY.sdo_srid SP_REF
FROM WHSE_FOREST_TENURE.FTEN_MAP_NOTATN_POINTS_SVW s
WHERE rownum = 1;


--Within Distance
SELECT
   aid.NAVIGATIONAL_AID_ID, ipr.INTRID_SID, ipr.TRANSACTION_SID,
   ROUND(SDO_GEOM.SDO_DISTANCE(ipr.SHAPE, aid.GEOMETRY, 0.005),2) DISTANCE
 
FROM
  WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES ipr,
  WHSE_ENVIRONMENTAL_MONITORING.CHRA_NAVIGATIONAL_AIDS_POINT aid

   WHERE aid.NAVIGATIONAL_AID_ID IN (700,701,704,708,684,678,675)
     AND  SDO_WITHIN_DISTANCE (ipr.SHAPE, aid.GEOMETRY, 'distance=200 unit=m') = 'TRUE';
     
 
 
 --Within Distance (with formatted output)
 SELECT b.PID, b.OWNER_TYPE, b.PARCEL_CLASS,
       CASE WHEN SDO_GEOM.SDO_DISTANCE(b.SHAPE, a.SHAPE, 0.5) = 0 
              THEN 'INTERSECT' 
                ELSE 'Within ' || TO_CHAR(ROUND(SDO_GEOM.SDO_DISTANCE(b.SHAPE, a.SHAPE, 0.5),0) || ' m')
                  END AS OVERLAY_RESULT

FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a, 
     WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_FA_SVW b

WHERE a.CROWN_LANDS_FILE = '1404764'
    AND a.DISPOSITION_TRANSACTION_SID = 937294
    AND a.INTRID_SID = 970611
    AND b.OWNER_TYPE = 'Private'
    AND SDO_WITHIN_DISTANCE (b.SHAPE, a.SHAPE,'distance = 500') = 'TRUE';
    
     
    
    
--Relate
SELECT
   ipr.INTRID_SID, pip.CNSLTN_AREA_NAME, pip.CONTACT_ORGANIZATION_NAME,
   ROUND((SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(pip.SHAPE,ipr.SHAPE, 0.005), 0.005, 'unit=HECTARE')/ 
   SDO_GEOM.SDO_AREA(ipr.SHAPE, 0.005, 'unit=HECTARE'))*100, 2) OVERLAP_PERCENT

FROM
    WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES ipr
      INNER JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip 
        ON SDO_RELATE (pip.SHAPE, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'

 WHERE pip.CNSLTN_AREA_NAME = q'[Hul'qumi'num Nations - Marine Territory]'
   AND ipr.INTRID_SID IN (XXXXXX);
   


-- Relate 2 (Nested Queries)
SELECT*

FROM
WHSE_TANTALIS.TA_CROWN_TENURES_SVW ipr
INNER JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip 
ON SDO_RELATE (pip.SHAPE, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'

WHERE ipr.INTRID_SID IN ( SELECT ipr2.INTRID_SID
                           FROM
                           WHSE_TANTALIS.TA_CROWN_TENURES_SVW ipr2
                             INNER JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip 
                           ON SDO_RELATE (pip.SHAPE, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                           
                            WHERE pip.CNSLTN_AREA_NAME = q'[Shishalh (Sechelt) First Nation]'
                            AND ipr2.INTRID_SID IN (XXXX)
                            );



---  Relate query with 3 GEO tables   
SELECT
   ipr.INTRID_SID, ipr.CROWN_LANDS_FILE, ipr.TENURE_SUBPURPOSE, ldn.LANDSCAPE_UNIT_NAME, pip.CONTACT_ORGANIZATION_NAME,
   ROUND((SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(ldn.GEOMETRY,ipr.SHAPE, 0.005), 0.005, 'unit=HECTARE')/ 
   SDO_GEOM.SDO_AREA(ipr.SHAPE, 0.005, 'unit=HECTARE'))*100, 2) OVERLAP_PERCENT

FROM
    WHSE_TANTALIS.TA_CROWN_TENURES_SVW ipr,
    WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip,
    WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW ldn

 WHERE pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
   AND ipr.TENURE_STAGE = 'TENURE' 
   AND ipr.TENURE_SUBPURPOSE = 'PRIVATE MOORAGE'
   AND SDO_RELATE (pip.SHAPE, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
   AND SDO_RELATE (ldn.GEOMETRY, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE' ;
   
   
   

-- Nearest Neighbor
SELECT ten.INTRID_SID, pp.PID, pp.OWNER_TYPE,  ROUND(SDO_NN_DISTANCE(1),1) PROXIMITY_METERS 

FROM WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_SVW pp,
    WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES ten

WHERE ten.INTRID_SID = XXXX
  AND pp.OWNER_TYPE = 'Private'
  AND SDO_NN(pp.SHAPE, ten.SHAPE, 'sdo_num_res={n_neighbor}' ,1) = 'TRUE'
   
   
   
   
 -- Nearest Neighbor. Distance between two Geometries
SELECT SDO_GEOM.SDO_DISTANCE(ten.SHAPE, SDO_GEOMETRY('{wkt}', {srid}), 0.005, 'unit=meter') PROXIMITY_METERS 
FROM WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES ten
WHERE ten.INTRID_SID = 134943;
