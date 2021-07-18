# wait for database to go online
sleep 15

# connect to database
PGPASSWORD=postgres psql -h postgres -U postgres postgres << EOF
\i schema.sql
\set content `cat /data/sc_data.json`
create temp table sc_document_raw( j json );
insert into sc_document_raw values (:'content');
insert into staging.sc_user_document select value as user_document from json_array_elements((select j from sc_document_raw limit 1));
drop table sc_document_raw;

\set content `cat /data/sc_user_event.json`
create temp table sc_event_raw( j json );
insert into sc_event_raw values (:'content');
insert into staging.sc_user_event select value as user_event from json_array_elements((select j from sc_event_raw limit 1));
drop table sc_event_raw;

\copy staging.crm_customer(customer_email,industry,region,company_name) FROM '/data/crm_data.csv' DELIMITER ',' CSV HEADER;
select sc.load_sc_user_event();
EOF
