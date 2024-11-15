SELECT * FROM public.house_info

select * FROM public.house_room

select * FROM public.house_transaction

-- Thêm khóa chính cho bảng house_info
ALTER TABLE house_info
ADD CONSTRAINT pk_house PRIMARY KEY ("House_ID");

-- Thêm khóa chính cho bảng public.house_room
ALTER TABLE public.house_room
ADD CONSTRAINT pk_room PRIMARY KEY ("house_room_ID");

-- Thêm khóa ngoại cho bảng public.house_room tham chiếu đến House_Info
ALTER TABLE public.house_room
ADD CONSTRAINT fk_room_id FOREIGN KEY ("House_ID")
REFERENCES House_Info("House_ID");

-- Thêm khóa chính cho bảng public.house_transaction
ALTER TABLE public.house_transaction
ADD CONSTRAINT pk_transaction PRIMARY KEY ("Transaction_ID");

-- Thêm khóa ngoại cho bảng public.house_room tham chiếu đến House_Info
ALTER TABLE public.house_transaction
ADD CONSTRAINT fk_transaction_id FOREIGN KEY ("House_ID")
REFERENCES House_Info("House_ID");

-- thực hiện thử các câu lệnh select join giữa các bảng
SELECT hi."House_ID", hi."Area", hi."Price",hr."Bedrooms", hr."Bathrooms"
FROM public.house_info hi JOIN public.house_room hr ON hi."House_ID" = hr."House_ID";

SELECT hi."House_ID", hi."Area", hi."Price", ht."Posted_Date", ht."Street_Name"
FROM public.house_info hi JOIN public.house_transaction ht ON hi."House_ID" = ht."House_ID";

--đổi kiểu dữ liệu cho khoái ngoại, khóa chính
ALTER TABLE public.house_info
ALTER COLUMN "House_ID" TYPE text 
INTEGER USING "House_ID"::INTEGER;

ALTER TABLE public.house_room
ALTER COLUMN "house_room_ID" TYPE text 
INTEGER USING "house_room_ID"::INTEGER;

ALTER TABLE public.house_transaction
ALTER COLUMN "Transaction_ID" TYPE text
INTEGER USING "house_room_ID"::INTEGER;

select "Area","Price","Bedrooms","Bathrooms","Posted_Date","Legal_Status","District_Name","City_Province_Name","Street_Name"
from public.house_info ht join public.house_room hr on ht."House_ID"=hr."House_ID" 
join public.house_transaction htr on ht."House_ID"=htr."House_ID"

CREATE EXTENSION dblink;


INSERT INTO public_house.house_data ("Area", "Price", "Bedrooms", "Bathrooms", "Posted_Date", "Legal_Status", "District_Name", "City_Province_Name", "Street_Name")
SELECT *
FROM dblink('dbname=postgre_bds user=postgres password=1',
            'SELECT "Area", "Price", "Bedrooms", "Bathrooms", "Posted_Date", "Legal_Status", "District_Name", "City_Province_Name", "Street_Name"
             FROM public.house_info ht
             JOIN public.house_room hr ON ht."House_ID" = hr."House_ID"
             JOIN public.house_transaction htr ON ht."House_ID" = htr."House_ID"')
AS source_data("Area" INTEGER, "Price" BIGINT, "Bedrooms" INTEGER, "Bathrooms" INTEGER, "Posted_Date" DATE, "Legal_Status" VARCHAR, "District_Name" VARCHAR, "City_Province_Name" VARCHAR, "Street_Name" VARCHAR);

--ngắt kết nối
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'mywarehouse' AND pid <> pg_backend_pid();

