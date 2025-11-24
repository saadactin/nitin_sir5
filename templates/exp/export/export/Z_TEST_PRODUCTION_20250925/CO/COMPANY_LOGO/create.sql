CREATE VIEW "Z_TEST_PRODUCTION_20250925"."COMPANY_LOGO" ( "logo" ) AS select
	 cast("BitmapPath" as nvarchar)||"LogoFile" "logo" 
from oadp