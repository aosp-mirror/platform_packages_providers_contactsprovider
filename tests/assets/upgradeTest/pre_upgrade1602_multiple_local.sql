DELETE FROM accounts;
DELETE FROM contacts;
DELETE FROM raw_contacts;
DELETE FROM groups;
DELETE FROM data;
DELETE FROM data_usage_stat;

--CREATE TABLE accounts (_id INTEGER PRIMARY KEY AUTOINCREMENT,account_name TEXT, account_type TEXT, data_set TEXT);

INSERT INTO "accounts" (_id, account_name, account_type, data_set) VALUES(1,"Other","NonLocalAccount",NULL);
INSERT INTO "accounts" (_id, account_name, account_type, data_set) VALUES(2,"Phone","LocalAccount",NULL);
INSERT INTO "accounts" (_id, account_name, account_type, data_set) VALUES(3,NULL,NULL,NULL);

--CREATE TABLE raw_contacts (_id INTEGER PRIMARY KEY AUTOINCREMENT,account_id INTEGER REFERENCES accounts(_id),sourceid TEXT,backup_id TEXT,raw_contact_is_read_only INTEGER NOT NULL DEFAULT 0,version INTEGER NOT NULL DEFAULT 1,dirty INTEGER NOT NULL DEFAULT 0,deleted INTEGER NOT NULL DEFAULT 0,metadata_dirty INTEGER NOT NULL DEFAULT 0,contact_id INTEGER REFERENCES contacts(_id),aggregation_mode INTEGER NOT NULL DEFAULT 0,aggregation_needed INTEGER NOT NULL DEFAULT 1,custom_ringtone TEXT,send_to_voicemail INTEGER NOT NULL DEFAULT 0,times_contacted INTEGER NOT NULL DEFAULT 0,last_time_contacted INTEGER,starred INTEGER NOT NULL DEFAULT 0,pinned INTEGER NOT NULL DEFAULT 0,display_name TEXT,display_name_alt TEXT,display_name_source INTEGER NOT NULL DEFAULT 0,phonetic_name TEXT,phonetic_name_style TEXT,sort_key TEXT COLLATE PHONEBOOK,phonebook_label TEXT,phonebook_bucket INTEGER,sort_key_alt TEXT COLLATE PHONEBOOK,phonebook_label_alt TEXT,phonebook_bucket_alt INTEGER,name_verified INTEGER NOT NULL DEFAULT 0,sync1 TEXT, sync2 TEXT, sync3 TEXT, sync4 TEXT );

INSERT INTO "raw_contacts" (_id, account_id) VALUES(1,1)
INSERT INTO "raw_contacts" (_id, account_id) VALUES(2,2)
INSERT INTO "raw_contacts" (_id, account_id) VALUES(3,3)
INSERT INTO "raw_contacts" (_id, account_id) VALUES(4,3)
INSERT INTO "raw_contacts" (_id, account_id) VALUES(5,1)
INSERT INTO "raw_contacts" (_id, account_id) VALUES(6,3)

--CREATE TABLE groups (_id INTEGER PRIMARY KEY AUTOINCREMENT,account_id INTEGER REFERENCES accounts(_id),package_id INTEGER REFERENCES package(_id),sourceid TEXT,version INTEGER NOT NULL DEFAULT 1,dirty INTEGER NOT NULL DEFAULT 0,title TEXT,title_res INTEGER,notes TEXT,system_id TEXT,deleted INTEGER NOT NULL DEFAULT 0,group_visible INTEGER NOT NULL DEFAULT 0,should_sync INTEGER NOT NULL DEFAULT 1,auto_add INTEGER NOT NULL DEFAULT 0,favorites INTEGER NOT NULL DEFAULT 0,group_is_read_only INTEGER NOT NULL DEFAULT 0,sync1 TEXT, sync2 TEXT, sync3 TEXT, sync4 TEXT )

INSERT INTO "groups" (_id, account_id) VALUES(1,1)
INSERT INTO "groups" (_id, account_id) VALUES(2,3)
