-- 1F. all HW (western hemlock) leading and secondary stands in ALL AGE classes in specific mapsheets on the coast. 
select 
    vri.objectId, 
    vri.LINE_3_TREE_SPECIES, 
    code1.DESCRIPTION as LEAD_AGE, 
    code2.description as LEAD_SP, 
    vri.project,
    vri.species_cd_1 as SP_1, 
    vri.species_pct_1 as PCT_1,
    vri.PROJ_AGE_CLASS_CD_1 as AGE_1, 
    vri.species_cd_2 as SP_2,
    vri.species_pct_2 as PCT_2, 
    vri.PROJ_AGE_CLASS_CD_2 as AGE_2, 
    sysdate as VRI_extraction_date,
    round(SDO_GEOM.SDO_AREA(geometry, 0.01) * 0.0001, 0) as Calc_Size_ha,
    case when vri.species_cd_1 = 'HW'
            then round(SDO_GEOM.SDO_AREA(geometry, 0.01) * 0.0001 * vri.species_pct_1 * 0.01, 1)
        when  vri.species_cd_2 = 'HW'
            then round(SDO_GEOM.SDO_AREA(geometry, 0.01) * 0.0001 * vri.species_pct_2 * 0.01, 1)
        else 99999 end as Ha_of_HW,
    
    geometry

from 
    WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY vri
        join WHSE_FOREST_VEGETATION.VEG_AGE_CLASS_CODE code1 
            on vri.PROJ_AGE_CLASS_CD_1 = code1.AGE_CLASS_CODE
        join WHSE_FOREST_VEGETATION.veg_tree_species_code code2 
            on vri.species_cd_1 = code2.tree_species_code

where 
    vri.PROJ_AGE_CLASS_CD_1 >= 1 -- get all age classes
    and (SPECIES_CD_1 = 'HW') or (SPECIES_CD_2 = 'HW')
    and SDO_ANYINTERACT(vri.geometry,
                (
                 select SDO_AGGR_UNION(SDOAGGRTYPE(m.GEOMETRY, 1)) as geom  -- tolerance is 1 map unit, i.e. 1 m
                 from WHSE_BASEMAPPING.BCGS_20K_GRID m
                 where m.MAP_TILE in ('092B023', '092B024', '092B031', '092B032')
                    )
                        ) = 'TRUE'
;
