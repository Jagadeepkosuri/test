update dev_staging.email_details set etl_status = 'Y' where etl_status = 'N';
update dev_staging.census_details set etl_status = 'Y' where etl_status = 'N';