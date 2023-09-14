-- intersect two tables
SELECT 
	water_licensing_watershed_name, 
	org_unit_n,
	ROUND((ST_Area(ST_Intersection(w.geom, d.geom))/10000)::numeric,2) AS intersection_area_ha

FROM
    water_licencing_watersheds AS w
JOIN
    district_regional_areas AS d
	ON
    	ST_Intersects(w.geom, d.geom)
	
WHERE 
	org_unit_n= 'South Island Natural Resource District';
	


-- intersect table and geometry (point)
SELECT 
    org_unit_n
FROM
    district_regional_areas AS d
WHERE
    ST_Intersects(ST_Transform(ST_GeomFromText('POINT(-124.874 48.792)', 4326), 3005), d.geom)
    AND org_unit_n = 'South Island Natural Resource District';
