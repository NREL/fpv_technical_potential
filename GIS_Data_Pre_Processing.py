
# Waterbodies

bash = """

ogrInfo /Users/XXXX/Desktop/floating_Solar/GIS/NHD.gdb

ogr2ogr -f "PostgreSQL" PG:"host=XXXX port=XXXX1 dbname=XXXX user=XXXX password=XXXX" "/Users/XXXX/Desktop/floating_Solar/GIS/NHD.gdb" "YYYY"

"""

bash = 'shp2pgsql -s {srid} -g the_geom_{srid} -c -D -I {file} {schema}.{table_name} | PGPASSWORD={password} psql -h {host} -p{port} {database} -U {user}'


import os
import shutil
import zipfile
# import geopandas as gpd

host = 'XXXX'  	
port = 'XXXX'			
database = 'XXXX'	
schema = 'fpv'		
user = 'XXXX'		
password = 'XXXX'	

file = "Shape/NHDWaterbody.shp"
zip_folders = os.listdir()
tables = []


for zip_folder in zip_folders:

	if '.zip' in zip_folder:		
		with zipfile.ZipFile(zip_folder,"r") as zip_ref:
			zip_ref.extractall()

		state = zip_folder[6:-10]
		table_name = state + '_waterbodies'
		tables.append(table_name)
		print (state)
		srid = 4269

		# g = gpd.GeoDataFrame.from_file(file)
		# if 'init' in g.crs.keys():
		# 	srid = g.crs['init'][5:]
		# if 'proj' in g.crs.keys():
		# 	proj = g.crs['proj']
		# 	print ("!!!!FLAG!!!!")
		# print ('-----')
		# print (g.crs)
		# print ('-----')

		bash = 'shp2pgsql -s {srid} -g the_geom_{srid} -c -D -I {file} {schema}.{table_name} | PGPASSWORD={password} psql -h {host} -p{port} {database} -U {user}'.format(**locals().copy())
		print (bash)
		os.system(bash)
		shutil.rmtree('Shape')


import pandas as pd 
from sqlalchemy import create_engine
from sqlalchemy.sql import text

#-- make connection
engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**locals().copy()))
con = engine.connect()

#-- standardize projection and fix geometries for all parcels
sql = """
	DROP TABLE IF EXISTS fpv.usa_waterbodies_gt_1acre;
	CREATE TABLE fpv.usa_waterbodies_gt_1acre (
		gid integer,
		areasqkm numeric,
		the_geom_4269 geometry
	);
""".format(**locals().copy())

con.execute(text(sql).execution_options(autocommit=True))

min_install_size = 0.00404686 	# Assumption: 1 acre in sqkm

for table_name in tables:

	print (table_name)
	state = table_name[:-20]

	sql = """
		WITH 
		a AS (
			SELECT
				state as '{state}',
				gid,
				areasqkm,
				the_geom_4269
			FROM fpv.{table_name}
			WHERE areasqkm > {min_install_size}
		)
		INSERT INTO fpv.usa_waterbodies_gt_1acre (SELECT * FROM a);
		""".format(**locals().copy())

	con.execute(text(sql).execution_options(autocommit=True))

con.close()

bash = """ pgsql2shp -f 'usa_waterbodies_gt_1acre.shp' -h XXXX -u XXXX -P XXXX XXXX "fpv.usa_waterbodies_gt_1acre" """










import pandas as pd 
from sqlalchemy import create_engine
from sqlalchemy.sql import text

#-- make connection
engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**locals().copy()))
con = engine.connect()

#-- standardize projection and fix geometries for all parcels
sql = """
	DROP TABLE IF EXISTS fpv.usa_waterbodies;
	CREATE TABLE fpv.usa_waterbodies_gt_1acre (
		gid integer,
		areasqkm numeric,
		the_geom_4269 geometry
	);
""".format(**locals().copy())

con.execute(text(sql).execution_options(autocommit=True))

min_install_size = 0.00404686 	# Assumption: 1 acre in sqkm

for table_name in tables:

	print (table_name)
	state = table_name[:-20]

	sql = """
		WITH 
		a AS (
			SELECT
				state as '{state}',
				gid,
				areasqkm,
				the_geom_4269
			FROM fpv.{table_name}
			WHERE areasqkm > {min_install_size}
		)
		INSERT INTO fpv.usa_waterbodies_gt_1acre (SELECT * FROM a);
		""".format(**locals().copy())

	con.execute(text(sql).execution_options(autocommit=True))

con.close()










# NID

bash = """
shp2pgsql -s 4326 -g the_geom_4326 -c -D -I NID2016.shp {schema}.nid | PGPASSWORD={password} psql -h {host} -p {port} {database} -U {user}
""".format(**locals().copy())
os.system(bash)

sql = """
ALTER TABLE fpv.nid ADD COLUMN purpose_exclusion text;
UPDATE fpv.nid
SET purpose_exclusion = (
	CASE
		WHEN purposes LIKE 'R' THEN 'True'
		WHEN purposes LIKE '%%F%%' THEN 'True'
		ELSE 'False'
	END );

ALTER TABLE fpv.nid ADD COLUMN recreation_flag text;
UPDATE fpv.nid
SET recreation_flag = (
	CASE
		WHEN purposes LIKE '%%R%%' THEN 'True'
		ELSE 'False'
	END );
"""

# QGIS to perform NNJoin w/ NID and transform polygons to centroids

shp2pgsql -s 4326 -g the_geom_4326 -c -D -I usa_waterbodies_gt_1acre_centroids_nid.shp fpv.usa_waterbodies_gt_1acre_centroids_nid | PGPASSWORD=XXXX psql -h XXXX -p XXXX XXXX -U XXXX


sql = """
SELECT count(*) from fpv.usa_waterbodies_gt_1acre;
select count(*) from fpv.usa_waterbodies_gt_1acre_centroids_nid;


select * from fpv.usa_waterbodies_gt_1acre_centroids_nid order by join_dam_n limit 200;
select count(*) from fpv.usa_waterbodies_gt_1acre_centroids_nid group by gid;
select * from fpv.usa_waterbodies_gt_1acre_centroids_nid limit 10;

create table fpv.join_distances as ( select nn_distanc from fpv.usa_waterbodies_gt_1acre_centroids_nid );



ALTER TABLE fpv.usa_waterbodies_gt_1acre_centroids_nid ADD COLUMN man_made text;
UPDATE fpv.usa_waterbodies_gt_1acre_centroids_nid
SET man_made = 'False';

create table fpv.man_made_gids AS (
    Select gid from 
     fpv.usa_waterbodies_gt_1acre_centroids_nid t
      inner join 
        (   Select join_dam_n, join_city, min(nn_distanc) as nn_distanc_min 
            from fpv.usa_waterbodies_gt_1acre_centroids_nid 
            group by join_dam_n, join_city
         ) xx 
        on t.join_city = xx.join_city and t.join_dam_n = xx.join_dam_n and t.nn_distanc = xx.nn_distanc_min
);

select * from fpv.man_made_gids limit 10;
    
UPDATE fpv.usa_waterbodies_gt_1acre_centroids_nid a
SET man_made = 'True'
WHERE a.gid in (SELECT gid from fpv.man_made_gids);


select count(*) from fpv.usa_waterbodies_gt_1acre_centroids_nid where man_made ='True';
select count(*) from fpv.nid;


ALTER TABLE fpv.usa_waterbodies_gt_1acre_centroids_nid ADD COLUMN height_exclusion text;
UPDATE fpv.usa_waterbodies_gt_1acre_centroids_nid
SET height_exclusion = (
	CASE
		WHEN join_nid_h < 7 THEN 'True'
		ELSE 'False'
	END );



CREATE TABLE fpv.usa_waterbodies_natural AS (
    select gid, areasqkm, the_geom_4326 
    from fpv.usa_waterbodies_gt_1acre_centroids_nid
    where man_made = 'False'
);

CREATE TABLE fpv.usa_waterbodies_manmade AS (
    select gid, join_dam_n as dam_name, join_state as state, join_purpo as purposes, join_own_1 as owner_type, join_recre as recreation_flag, join_surfa as nid_area, (areasqkm *247.105) as nhd_area, the_geom_4326 
    from fpv.usa_waterbodies_gt_1acre_centroids_nid
    where man_made = 'True' and height_exclusion = 'False' and join_pur_1 = 'False' 
);

update fpv.usa_waterbodies_manmade
set nid_area = nhd_area
where nid_area = 0;

create table fpv.man_made_area_differences AS (
select (100 * nid_area / nhd_area) from fpv.usa_waterbodies_manmade where nid_area != nhd_area
);
"""







shp2pgsql -s 4326 -g the_geom_4326 -c -D -I usa_waterbodies_manmade_ghi_evaporation.shp fpv.usa_waterbodies_manmade_ghi_evaporation | PGPASSWORD=XXXX psql -h XXXX -p XXXX XXXX -U XXXX







# Transmission Lines

bash = """
pgsql2shp -f 'electric_transmission_lines_20131120.shp' -h XXXX -u XXXX -P XXXX XXXX "select * from ventyx.electric_transmission_lines_20131120"
"""

sql = """

CREATE TABLE XXXX.electric_transmission_lines_20131120
AS (
	SELECT * FROM ventyx.electric_transmission_lines_20131120 
	);

-- add column for standardized geometry in 5070
ALTER TABLE XXXX.electric_transmission_lines_20131120 ADD COLUMN the_geom_5070 geometry;
UPDATE XXXX.electric_transmission_lines_20131120
SET the_geom_5070 = st_transform(the_geom_4326, 5070);

-- update column for standardized geometry with buffer in 5070
UPDATE XXXX.electric_transmission_lines_20131120
SET the_geom_5070 = st_buffer(the_geom_5070, (80467.2) );    -- 50 miles = 80467.2 m

-- add column for standardized geometry with buffer in 4326
ALTER TABLE XXXX.electric_transmission_lines_20131120 ADD COLUMN the_geom_buffer_4326 geometry;
UPDATE XXXX.electric_transmission_lines_20131120
SET the_geom_buffer_4326 = st_transform(the_geom_5070, 4326);

-- update buffer geometries in 4326
UPDATE XXXX.electric_transmission_lines_20131120
SET the_geom_buffer_4326 = st_collectionextract(st_makevalid(the_geom_buffer_4326), 3)
WHERE st_isvalid(the_geom_buffer_4326) IS false;

"""

# Used QGIS to dissolve polygons

bash = """
shp2pgsql -s 4326 -g the_geom_4326 -c -D -I transmission_exclusion_clipped.shp {schema}.transmission_exclusion_clipped | PGPASSWORD={password} psql -h {host} -p {port} {database} -U {user}
""".format(**locals().copy())




shp2pgsql -s 4326 -g the_geom_4326 -c -D -I usa_waterbodies_manmade_temp1.shp fpv.usa_waterbodies_manmade_temp1 | PGPASSWORD=XXXX psql -h XXXX -p XXXX XXXX -U XXXX
shp2pgsql -s 4326 -g the_geom_4326 -c -D -I usa_waterbodies_natural_temp1.shp fpv.usa_waterbodies_natural_temp1 | PGPASSWORD=XXXX psql -h XXXX -p XXXX XXXX -U XXXX


shp2pgsql -s 4326 -g the_geom_4326 -c -D -I us_states_contiguous.shp fpv.us_states_contiguous | PGPASSWORD=XXXX psql -h XXXX -p XXXX XXXX -U XXXX


pgsql2shp -f 'usa_waterbodies_manmade_temp1.shp' -h XXXX -u XXXX -P XXXX XXXX "select * from ventyx.electric_transmission_lines_20131120"



# SRDB

sql = """
CREATE EXTENSION dblink;
CREATE TABLE fpv.naris_nrel_psm_ghi_raster AS (
    SELECT x.*
    FROM dblink('dbname=XXXX user=XXXX password=XXXX host=XXXX',
        'SELECT * FROM solar.naris_nrel_psm_ghi_raster')
    AS x(
        rid integer,
        rast raster
	)
);
"""
con.execute(text(sql).execution_options(autocommit=True))

sql = """
CREATE TABLE fpv.usa_waterbodies_gt_1acre_use_ghi AS (
	SELECT a.*, st_value(b.rast, a.the_point_4326) as ghi
	FROM fpv.usa_waterbodies_gt_1acre_use a
    LEFT JOIN fpv.naris_nrel_psm_ghi_raster b
    ON st_intersects(b.rast, a.the_point_4326)
);
"""
con.execute(text(sql).execution_options(autocommit=True))





# Capacity / Generation

lake_percentages = [5, 15, 25, 50]   	# Range of Assumptions
capacity_multiplier = 0000 # kW / m2	# Get from current systems
efficiency = 0.19						# Get from current systems
sys_losses = 0.96

for perc in lake_percentages:

	sql = """
	ALTER TABLE fpv.usa_waterbodies_gt_1acre_use_ghi ADD COLUMN installation_areasqkm_{perc} numeric;
	UPDATE fpv.usa_waterbodies_gt_1acre_use_ghi
	SET installation_areasqkm_{perc} = {perc} * areasqkm / 100;

	ALTER TABLE fpv.usa_waterbodies_gt_1acre_use_ghi ADD COLUMN installation_capacity_{perc} numeric;
	UPDATE fpv.usa_waterbodies_gt_1acre_use_ghi
	SET installation_capacity_{perc} = installation_areasqkm_{perc} * {capacity_multiplier};

	ALTER TABLE fpv.usa_waterbodies_gt_1acre_use_ghi ADD COLUMN installation_generation_{perc} numeric;
	UPDATE fpv.usa_waterbodies_gt_1acre_use_ghi
	SET installation_generation_{perc} = installation_areasqkm_{perc} * ghi * {efficiency} * {sys_losses};


	SELECT SUM(installation_areasqkm_{perc}), SUM(installation_capacity_{perc}), SUM(installation_generation_{perc})
	FROM fpv.usa_waterbodies_gt_1acre_use_ghi
	WHERE installation_areasqkm_{perc} > {min_install_size};
	""".format(**locals().copy())

	con.execute(text(sql).execution_options(autocommit=True))









# PROCESS MANMADE

SQL = """

CREATE table fpv.usa_waterbodies_manmade_ghi AS (
    select a.*,
    st_value(b.rast, a.the_geom_4326) as ghi
    from fpv.usa_waterbodies_manmade_temp1 a
    left join fpv.naris_nrel_psm_ghi_raster b
    ON st_intersects (a.the_geom_4326, b.rast)  
    );
    
create table fpv.usa_waterbodies_natural_ghi AS (
    select a.*,
    st_value(b.rast, a.the_geom_4326) as ghi
    from fpv.usa_waterbodies_natural_temp1 a
    left join fpv.naris_nrel_psm_ghi_raster b
    ON st_intersects (b.rast, a.the_geom_4326)  
    );

create table fpv.waterbody_area_distribution as (
    select nid_area from fpv.usa_waterbodies_manmade_ghi
    );

ALTER TABLE fpv.usa_waterbodies_manmade_ghi ADD COLUMN generation numeric;
UPDATE fpv.usa_waterbodies_manmade_ghi
SET generation = ( 0.27 * 0.19 * 0.96 * 4046.86 * nid_area * ghi ) ;



CREATE TABLE fpv.land_value_utility_rates_state 
(state varchar, land numeric, pasture numeric, cropland numeric, utility numeric);

COPY fpv.land_value_utility_rates_state FROM '/Volumes/LaCie/FPV_GIS/state_land_value_2016.csv' DELIMITER ',' CSV;

create table fpv.land_utility_state as (
select a.state, a.pasture, a.cropland, a.utility, b.the_geom_4326 
from fpv.land_value_utility_rates_state a
left join fpv.us_states_contiguous b
on a.state = b.stusps
        );







CREATE table fpv.owner_to_type as (
    select dam_name, state, owner, owner_type
    from (
        select gid, join_dam_n as dam_name, join_state as state, join_owner as owner, join_own_1 as owner_type
		from fpv.usa_waterbodies_gt_1acre_centroids_nid
		where man_made = 'True' and height_exclusion = 'False' and join_pur_1 = 'False' 
        ) foo
    group by dam_name, state, owner, owner_type
);


create table fpv.usa_waterbodies_manmade_ghi_evap4 AS (
    select a.*, b.owner_type as own_type
    from fpv.usa_waterbodies_manmade_ghi_evaporation a
    right join fpv.owner_to_type b
    ON a.state=b.state and a.dam_name = b.dam_name
    );
    

create table fpv.usa_waterbodies_manmade_purpose_owner as (
    select own_type, purposes, sum(generation) as generation from
    (
        select dam_name, state, own_type, purposes, generation 
        from fpv.usa_waterbodies_manmade_ghi_evap4
        group by dam_name, state, own_type, purposes, generation 
    ) foo
    group by own_type, purposes
);

####################### Group owners and purposes

select count(*) from fpv.usa_waterbodies_manmade_ghi_evap4;

create table fpv.usa_waterbodies_manmade_evap as (
    select gid, dam_name, state, purposes, own_type as owners, 
    nid_area, nhd_area, ghi, evap_acref as evap_acrft, the_geom_4326
    from fpv.usa_waterbodies_manmade_ghi_evap4
    where purposes not like '%R%'
    and purposes not like '%N%'
    and purposes not like '%X%'
    and purposes not like '%O%'
    and own_type not like '%X%'
    );
    
select * from fpv.usa_waterbodies_manmade_evap order by random() limit 20;

alter table fpv.usa_waterbodies_manmade_evap add column prim_purp text;
update fpv.usa_waterbodies_manmade_evap
set prim_purp = SUBSTRING (purposes, 1, 1);

alter table fpv.usa_waterbodies_manmade_evap add column prim_own text;
update fpv.usa_waterbodies_manmade_evap
set prim_own = SUBSTRING (owners, 1, 1);

alter table fpv.usa_waterbodies_manmade_evap add column prim_purpose text;
update fpv.usa_waterbodies_manmade_evap
set prim_purpose = (
case
    when prim_purp = 'I' then 'Irrigation'
    when prim_purp = 'H' then 'Hydroelectric'
    when prim_purp = 'S' then 'Water Supply'
    when prim_purp = 'T' then 'Tailings'
    when prim_purp = 'P' then 'Control, Stabilization, Protection'
    when prim_purp = 'D' then 'Control, Stabilization, Protection'
    when prim_purp = 'C' then 'Control, Stabilization, Protection'
    when prim_purp = 'G' then 'Control, Stabilization, Protection'
    else 'Error'
    end );

alter table fpv.usa_waterbodies_manmade_evap add column prim_owner text;
update fpv.usa_waterbodies_manmade_evap
set prim_owner = (
case
    when prim_own = 'F' then 'Federal'
    when prim_own = 'S' then 'State'
    when prim_own = 'L' then 'Local Government'
    when prim_own = 'U' then 'Public Utility'
    when prim_own = 'P' then 'Private'
    else 'Error'
    end );
    
alter table fpv.usa_waterbodies_manmade_evap drop column prim_purp;
alter table fpv.usa_waterbodies_manmade_evap drop column prim_own;

select * from fpv.usa_waterbodies_manmade_evap order by random() limit 20;

########################







create table fpv.usa_waterbodies_manmade_ghi_evap_states as (
    select state, sum(nid_area) as surf_area, sum(generation) as generation, sum(evap_acref) as evap_acrft from
    (
        select dam_name, state, nid_area, generation, evap_acref 
        from fpv.usa_waterbodies_manmade_ghi_evaporation
        group by dam_name, state, nid_area, generation, evap_acref
    ) foo
    group by state
);

create table fpv.usa_waterbodies_manmade_state as (
    select a.*, b.pasture, b.cropland, b.utility, b.the_geom_4326
    from fpv.usa_waterbodies_manmade_ghi_evap_states a
    left join fpv.land_utility_state b
    on a.state = b.state
    );

alter table fpv.usa_waterbodies_manmade_state add column evap_cost numeric;
update fpv.usa_waterbodies_manmade_state
set evap_cost = 0.27 * evap_acrft * (1.50 / 1000) * 325851;

alter table fpv.usa_waterbodies_manmade_state add column util_val numeric;
update fpv.usa_waterbodies_manmade_state
set util_val = generation * (utility / 100) * 365;

alter table fpv.usa_waterbodies_manmade_state add column land_val numeric;
update fpv.usa_waterbodies_manmade_state
set land_val = 0.27 * surf_area * ( ( pasture + cropland ) / 2 );

"""











