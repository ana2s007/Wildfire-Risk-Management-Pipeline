import psycopg2

#Creates connection
connection=psycopg2.connect(user="user",
password="password",
host="",
port="",
database="database")

cursor=connection.cursor()

#Creates fire table in PostgreSql database
create_table_fires = (""" CREATE TABLE IF NOT EXISTS fires(
        OBJECTID int, SOURCE_SYSTEM_TYPE VARCHAR(250) ,
        SOURCE_SYSTEM VARCHAR(250),
        NWCG_REPORTING_UNIT_NAME VARCHAR(250),
        SOURCE_REPORTING_UNIT VARCHAR(250),
        SOURCE_REPORTING_UNIT_NAME VARCHAR(250),
        LOCAL_FIRE_REPORT_ID VARCHAR(250),
        FIRE_CODE VARCHAR(250),FIRE_NAME VARCHAR(250),
        FIRE_YEAR int, FIRE_SIZE int,
        FIRE_SIZE_CLASS VARCHAR(250),
        LATITUDE VARCHAR(250), LONGITUDE VARCHAR(250),
        OWNER_CODE int, OWNER_DESCR VARCHAR(250),
        STATE VARCHAR(250), COUNTY VARCHAR(250),
        STATE_NAME VARCHAR(50));""")
cursor.execute(create_table_fires)

#Creates plot_condition table
create_table_plot_condition = ("""CREATE TABLE IF NOT EXISTS plot_condition(
        plot_sequence_number bigint, plot_inventory_year int,
        plot_state_code bigint, plot_state_code_name VARCHAR(250),
        plot_survey_unit_code bigint, plot_county_code int,
        plot_design_code bigint, plot_statud_code int,
        plot_status_code_name VARCHAR(250),
        plot_nonsampled_reason_code int,
        plot_nonsampled_reason_code_name VARCHAR(250),
        latitude real, longitude real,
        elevation int, intensity int,
        p2_vegetation_sampling_status_code int,
        p2_vegetation_sampling_status_code_name varchar(250),
        inventory_year int, state_code_name VARCHAR(250),
        county_code int, county_code_name VARCHAR(250),
        forest_type_code int, forest_type_code_name VARCHAR(250),
        field_forest_type_code int, field_forest_type_code_name VARCHAR(250),
        mapping_density int, mapping_density_name VARCHAR(250),
        stand_age int, condition_proportion_unadjusted real,
        microplot_proportion_unadjusted real, subplot_proportion_unadjusted real,
        macroplot_proportion_unadjusted real, slope int,
        aspect int, basal_area_per_acre_of_live_trees real,
        fieldrecorded_stand_age int, alllivetree_stocking_percent real,
        growingstock_stocking_percent real
        );""")
cursor.execute(create_table_plot_condition)

#Creates plot table_name
create_table_plot = (""" CREATE TABLE IF NOT EXISTS plot(
        plot_sequence_number bigint, survey_sequence_number bigint,
        county_sequence_number bigint, plot_inventory_year bigint,
        plot_state_code bigint,
        plot_state_code_name VARCHAR(50),
        plot_survey_unit_code bigint,
                            plot_county_code bigint,
                            plot_phase_2_plot_number bigint,
                            plot_status_code bigint,
                            measurement_year bigint,
                            sample_kind_code bigint,
                            sample_kind_code_name VARCHAR(250),
                            plot_design_code bigint,
                            latitude real,
                            longitude real,
                            elevation bigint,
                            p2_vegetation_sampling_status_code bigint,
                            p2_vegetation_sampling_status_code_name VARCHAR(250),
                            p2_vegetation_sampling_level_detail_code bigint,
]                            p2_vegetation_sampling_level_detail_code_name VARCHAR(250),
                            unique_plot VARCHAR(250),
                            precipitation VARCHAR(250));""")
    cursor.execute(create_table_plot)
