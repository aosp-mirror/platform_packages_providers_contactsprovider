DELETE FROM accounts;
DELETE FROM contacts;
DELETE FROM raw_contacts;
DELETE FROM data;
DELETE FROM data_usage_stat;

--CREATE TABLE accounts (_id INTEGER PRIMARY KEY AUTOINCREMENT,account_name TEXT, account_type TEXT, data_set TEXT);

INSERT INTO "accounts" (_id, account_name, account_type, data_set) VALUES(1,"Phone","LocalAccount",NULL);
INSERT INTO "accounts" (_id, account_name, account_type, data_set) VALUES(2,"Phone","LocalAccount","local_data_set");
INSERT INTO "accounts" (_id, account_name, account_type, data_set) VALUES(3,"Phone2","LocalAccount",NULL);
INSERT INTO "accounts" (_id, account_name, account_type, data_set) VALUES(4,"Phone","NonLocalAccount",NULL);
INSERT INTO "accounts" (_id, account_name, account_type, data_set) VALUES(5,"Other","NonLocalAccount",NULL);
