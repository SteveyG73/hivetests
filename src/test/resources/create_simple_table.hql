USE ${hiveconf:database};

CREATE TABLE IF NOT EXISTS ${hiveconf:database}.sg_basic_text_table
(id int,
 name string,
 description string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
