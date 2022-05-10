DELETE FROM accounts;
DELETE FROM settings;
DELETE FROM contacts;
DELETE FROM raw_contacts;
DELETE FROM data;
DELETE FROM data_usage_stat;

--CREATE TABLE accounts (_id INTEGER PRIMARY KEY AUTOINCREMENT,account_name TEXT, account_type TEXT, data_set TEXT);

INSERT INTO "accounts" VALUES(1,NULL,NULL,NULL);
INSERT INTO "accounts" VALUES(2,"visible","type1",NULL);
INSERT INTO "accounts" VALUES(3,"visible","type1","ds_not_visible");
INSERT INTO "accounts" VALUES(4,"not_syncable","type1",NULL);
INSERT INTO "accounts" VALUES(5,"no_settings","type2",NULL);

--CREATE TABLE settings (account_name STRING NOT NULL,account_type STRING NOT NULL,data_set STRING,ungrouped_visible INTEGER NOT NULL DEFAULT 0,should_sync INTEGER NOT NULL DEFAULT 1);

INSERT INTO "settings" VALUES ("visible","type1",NULL,1,1)
INSERT INTO "settings" VALUES ("visible","type1","ds_not_visible",0,1)
INSERT INTO "settings" VALUES ("not_syncable","type1",NULL,0,0)
