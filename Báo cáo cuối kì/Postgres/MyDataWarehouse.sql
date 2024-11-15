-- Database: mydatawarehouse

-- DROP DATABASE IF EXISTS mydatawarehouse;

CREATE DATABASE mydatawarehouse
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;



CREATE EXTENSION dblink;
CREATE TABLE public.property_analysis (
    "Area" integer NOT NULL,
    "Price" bigint,
    "Bedrooms" integer NOT NULL,
    "Bathrooms" integer NOT NULL,
    "District_Name" text,
    "City_Province_Name" text,
    "Posted_Date" date,
    "Legal_Status" text NOT NULL
    -- "Street_Name" text
);  


-- Sử dụng dblink để sao chép dữ liệu từ cơ sở dữ liệu cũ
SELECT * 
FROM dblink(
    'host=localhost dbname=postgre_bds user=postgres password=1210',
    'SELECT "Area", "Price", "Bedrooms", "Bathrooms", "Posted_Date", "Legal_Status", "District_Name", "City_Province_Name"
     FROM public.house_info ht
     JOIN public.house_room hr ON ht."House_ID" = hr."House_ID"
     JOIN public.house_transaction htr ON ht."House_ID" = htr."House_ID"'
) AS data(
    "Area" integer,
    "Price" bigint,
    "Bedrooms" integer,
    "Bathrooms" integer,
    "Posted_Date" date,
    "Legal_Status" text,
    "District_Name" text,
    "City_Province_Name" text
    -- "Street_Name" text
);


							  
INSERT INTO public.property_analysis (
    "Area",
    "Price",
    "Bedrooms",
    "Bathrooms",
    "District_Name",
    "City_Province_Name",
    "Posted_Date",
    "Legal_Status"
    -- "Street_Name"
)
SELECT
    result."Area",
    result."Price",
    result."Bedrooms",
    result."Bathrooms",
    result."District_Name",
    result."City_Province_Name",
    result."Posted_Date",
    result."Legal_Status"
    -- result."Street_Name"
FROM 
    dblink(
        'host=localhost dbname=postgre_bds user=postgres password=1210', 
        'SELECT "Area", "Price", "Bedrooms", "Bathrooms", "District_Name", "City_Province_Name", "Posted_Date", "Legal_Status"
         FROM public.house_info ht
         JOIN public.house_room hr ON ht."House_ID" = hr."House_ID"
         JOIN public.house_transaction htr ON ht."House_ID" = htr."House_ID"'
    ) AS result(
        "Area" integer,
        "Price" bigint,
        "Bedrooms" integer,
        "Bathrooms" integer,
        "District_Name" text,
        "City_Province_Name" text,
        "Posted_Date" date,
        "Legal_Status" text
        -- "Street_Name" text
    );

select * from public.property_analysis
	