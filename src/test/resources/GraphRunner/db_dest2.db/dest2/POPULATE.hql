INSERT OVERWRITE TABLE db_dest2.dest2 PARTITION (partCol1, partCol2)
SELECT * FROM db_dest2.dest


