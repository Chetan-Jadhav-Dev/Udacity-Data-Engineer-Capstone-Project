class PostgresSqlQueries:
    """
    Contains SQL queries used to build schema and extract data
    """
    # Create Countries Table
    create_countries_table = """
    DROP TABLE IF EXISTS public.dim_countries CASCADE;
    CREATE TABLE public.dim_countries
    (
    country_id bigint GENERATED ALWAYS AS IDENTITY,
    country_code varchar(3) NOT NULL UNIQUE,
    country varchar(256) NOT NULL UNIQUE,
    average_temperature NUMERIC(16,3) NULL,
    PRIMARY KEY(country_id)
    );
    """

    # Extract countries from staging immigration data
    extract_countries = """
    INSERT INTO public.dim_countries (country_code, country, average_temperature)
    (SELECT DISTINCT c.country_code, c.country, t."AverageTemperature"
    FROM public.staging_immigration i
    INNER JOIN public.staging_countries c ON i.country_code = c.country_code
    LEFT JOIN public.staging_country_temperatures t ON UPPER(c.country) = UPPER(t."Country")
    ORDER BY c.country) 
    ON CONFLICT (country)
    DO NOTHING
    """

    # Create Ports Dimension Table
    create_ports_table = """
    DROP TABLE IF EXISTS public.dim_ports CASCADE;
    CREATE TABLE public.dim_ports
    (
    port_id BIGINT GENERATED ALWAYS AS IDENTITY,
    port_code VARCHAR(3) UNIQUE,
    port_city VARCHAR(256),
    port_state VARCHAR(50),
    average_temperature NUMERIC(16,3) NULL,
    PRIMARY KEY(port_id)
    );
    """

    # Create Airports Dimension Table
    create_airports_table = """
    DROP TABLE IF EXISTS public.dim_airports CASCADE;
    CREATE TABLE public.dim_airports
    (
    airport_id BIGINT GENERATED ALWAYS AS IDENTITY,
    port_id BIGINT UNIQUE,
    airport_type VARCHAR(256),
    airport_name VARCHAR(256),
    elevation_ft INT,
    municipality VARCHAR(256),
    gps_code VARCHAR(256),
    iata_code VARCHAR(256),
    local_code VARCHAR(256),
    coordinates VARCHAR(256),
    PRIMARY KEY(airport_id),
    CONSTRAINT fk_port
    FOREIGN KEY(port_id) REFERENCES dim_ports(port_id)
    );
    """

    # Extract ports from staging immigration data
    extract_ports = """
    INSERT INTO public.dim_ports (port_code, port_city, port_state, average_temperature)
    (SELECT DISTINCT p.port_code, p.city, p.state, t."AverageTemperature"
    FROM public.staging_immigration i
    INNER JOIN public.staging_ports p ON i.port_code = p.port_code
    LEFT JOIN public.staging_city_temperatures t ON UPPER(p.city) = UPPER(t."City")
    ORDER BY p.port_code)
    ON CONFLICT (port_code) DO NOTHING
    """

    # Extract airports data from staging airports 
    extract_airports = """
    INSERT INTO public.dim_airports (port_id, airport_type, airport_name, elevation_ft, municipality, gps_code,
    iata_code, local_code, coordinates)
    (SELECT p.port_id, a.type, a.name, a.elevation_ft, a.municipality,
    a.gps_code, a.iata_code, a.local_code, a.coordinates
    FROM public.staging_airports a
    INNER JOIN public.dim_ports p ON a.ident = p.port_code
    ORDER BY p.port_code)
    ON CONFLICT (port_id) DO NOTHING
    """

    # Create demographics dimension table
    create_demographics_table = """
    DROP TABLE IF EXISTS public.dim_demographics CASCADE;
    CREATE TABLE public.dim_demographics
    (
    demographics_id BIGINT GENERATED ALWAYS AS IDENTITY,
    port_id BIGINT,
    median_age numeric(18,2),
    male_population int,
    female_population int,
    total_population bigint,
    number_of_veterans int,
    foreign_born int,
    avg_household_size numeric(18,2),
    race varchar(100),
    demo_count int,
    UNIQUE (port_id, race),
    PRIMARY KEY(demographics_id),
    CONSTRAINT fk_port
    FOREIGN KEY(port_id) REFERENCES dim_ports(port_id)
    );
    """

    # Extract demographics from staging data
    extract_demographics = """
    INSERT INTO public.dim_demographics (port_id, median_age, male_population, 
    female_population, total_population, number_of_veterans,foreign_born,avg_household_size, race, demo_count)
    (SELECT DISTINCT p.port_id, d."Median Age",d."Male Population",d."Female Population",d."Total Population",
    d."Number of Veterans", d."Foreign-born",d."Average Household Size",d.race, d."count"
    FROM public.dim_ports p
    INNER JOIN public.staging_demographics d 
    ON UPPER(p.port_city) = UPPER(d.city) AND UPPER(p.port_state) = UPPER(d."State Code")
    WHERE EXISTS (SELECT port_code FROM public.staging_immigration i 
    WHERE p.port_code = i.port_code))
    ON CONFLICT (port_id,race) DO NOTHING
    """

    # Create time dimension table
    create_time_table = """
    DROP TABLE IF EXISTS public.dim_time CASCADE;
    CREATE TABLE public.dim_time
    (
    sas_timestamp INT NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    day_of_week INT NOT NULL,
    quarter INT NOT NULL,
    PRIMARY KEY (sas_timestamp)
    );
    """

    # Extract time dimension data from staging immigration arrival and departure dates
    extract_time_data = """
    INSERT INTO public.dim_time (sas_timestamp, year,month,day,quarter,week,day_of_week)
    SELECT ts, 
    date_part('year', sas_date) as year,
    date_part('month', sas_date) as month,
    date_part('day', sas_date) as day, 
    date_part('quarter', sas_date) as quarter,
    date_part('week', sas_date) as week,
    date_part('dow', sas_date) as day_of_week
    FROM
    (SELECT DISTINCT arrdate as ts, TIMESTAMP '1960-01-01 00:00:00 +00:00' + (arrdate * INTERVAL '1 day') as sas_date
    FROM staging_immigration
    UNION
    SELECT DISTINCT depdate as ts, TIMESTAMP '1960-01-01 00:00:00 +00:00' + (depdate * INTERVAL '1 day') as sas_date
    FROM staging_immigration
    WHERE depdate IS NOT NULL
    ) t1
    ON CONFLICT(sas_timestamp) DO NOTHING
    """

    # Create fact immigration table
    create_fact_immigration_table = """
    DROP TABLE IF EXISTS public.fact_immigration CASCADE;
    CREATE TABLE public.fact_immigration
    (
    immigration_id BIGINT GENERATED ALWAYS AS IDENTITY,
    country_id BIGINT,
    port_id BIGINT,
    age int,
    travel_mode varchar(100),
    visa_category varchar(100),
    visa_type varchar(100),
    gender varchar(10),
    birth_year int,
    arrdate int NOT NULL,
    depdate int NULL,
    PRIMARY KEY (immigration_id),
    CONSTRAINT fk_port FOREIGN KEY(port_id) REFERENCES dim_ports(port_id),
    CONSTRAINT fk_country FOREIGN KEY(country_id) REFERENCES dim_countries(country_id),
    CONSTRAINT fk_arrdate FOREIGN KEY(arrdate) REFERENCES dim_time(sas_timestamp),
    CONSTRAINT fk_depdate FOREIGN KEY(depdate) REFERENCES dim_time(sas_timestamp)
    );
    """

    # Extract immigration data from staging to fact table
    extract_immigration_data = """
    INSERT INTO public.fact_immigration (country_id, port_id, age, travel_mode, visa_category, visa_type,
                                        gender,birth_year,arrdate,depdate)
    SELECT c.country_id, p.port_id, i.age, i.mode, i.visa_category,i.visatype,i.gender, i.birth_year,
    i.arrdate, i.depdate
    FROM public.staging_immigration i
    INNER JOIN public.dim_countries c ON i.country_code = c.country_code
    INNER JOIN public.dim_ports p ON i.port_code = p.port_code
    """

    # Create staging immigration table
    create_staging_immigration = """
    DROP TABLE IF EXISTS public.staging_immigration;
    CREATE TABLE public.staging_immigration (
    year int2,
    month int2,
    arrival_day int2,
    age int2,
    country_code varchar(3),
    port_code varchar(3),
    mode varchar(256),
    visa_category varchar(256),
    visatype varchar(128),
    gender varchar(10) null,
    birth_year int2,
    arrdate int4,
    arrival_date date,
    depdate int4 null,
    departure_date date null
    );
    """

    # Create staging countries table
    create_staging_countries = """
    DROP TABLE IF EXISTS public.staging_countries;
    CREATE TABLE public.staging_countries (
    country_code varchar(3) NOT NULL,
    country varchar(256) NOT NULL
    );
    """

    # Create staging ports table
    create_staging_ports_table = """
    DROP TABLE IF EXISTS public.staging_ports;
    CREATE TABLE public.staging_ports (
    port_code varchar(3),
    city varchar(256),
    state varchar(50)
    );
    """

    # Create staging airports table
    create_staging_airports_table = """
    DROP TABLE IF EXISTS public.staging_airports;
    CREATE TABLE public.staging_airports (
    ident varchar(256),
    type varchar(256),
    name varchar(256),
    elevation_ft int4,
    continent varchar(256),
    iso_country varchar(256),
    iso_region varchar(256),
    municipality varchar(256),
    gps_code varchar(256),
    iata_code varchar(256),
    local_code varchar(256),
    coordinates varchar(256)
    );
    """

    # Create demographics staging table
    create_staging_demographics_table = """
    DROP TABLE IF EXISTS public.staging_demographics;
    CREATE TABLE public.staging_demographics (
    City varchar(256),
    State varchar(100),
    "Median Age" numeric(18,2),
    "Male Population" int4,
    "Female Population" int4,
    "Total Population" int8,
    "Number of Veterans" int4,
    "Foreign-born" int4,
    "Average Household Size" numeric(18,2),
    "State Code" varchar(50),
    Race varchar(100),
    Count int4
    );
    """

    # Staging to fact table data quality check
    # Ensures that qualifying staging items make it to fact table
    staging_to_fact_data_quality_check = """
    SELECT s.stagingCount - f.factCount FROM
    (SELECT COUNT(i.*) as stagingCount
    FROM public.staging_immigration i
    INNER JOIN public.dim_countries c ON i.country_code = c.country_code
    INNER JOIN public.dim_ports p ON i.port_code = p.port_code) s
    CROSS JOIN (SELECT COUNT(*) as factCount FROM fact_immigration i INNER JOIN dim_time t ON i.arrdate = t.sas_timestamp 
    WHERE t.year={{ execution_date.year }} AND t.month={{execution_date.month}} and t.day={{ execution_date.day }}) f
    """

    # Staging count data quality check
    # Ensure items are in staging table
    staging_count_data_quality_check = """
    SELECT COUNT(*) FROM staging_immigration
    """
