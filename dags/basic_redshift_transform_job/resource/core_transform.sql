drop table if exists dev_core.reporting;

create table  dev_core.reporting as
select a.id,a.name,
        a.updated_time as census_updated_time,
        a.state,
        state_id,
        email,
        b.updated_time as email_updated_time,
        case when email_type = 'Billing' THEN  'office_email' else email_type end as email_type
FROM dev_staging.census_details a
INNER JOIN dev_staging.email_details b on a.id = b.id ;






;



