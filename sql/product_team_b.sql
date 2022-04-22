create or replace table product_team_b.product_catalog_with_no_img_samsung_after_2000 as
SELECT *
FROM `och-sandbox.staging.gcs_product_catalog`
where lower(brand)='samsung'
and image is null
and year_release >2000