create or replace table product_team_a.product_catalog_with_no_img as
SELECT *
FROM `och-sandbox.staging.gcs_product_catalog`
where image is null