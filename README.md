# Hive_Migration_1.1
migration hive data
step.1 Generate table creation statement based on table file
step.2 Modify the original table creation statement to create the target table in the target hive cluster
step.3 Generate hdfs storage path according to configured hive table and hdfs information
step.4 Distcp hdfs data, repair hive partition data
step.5 Generate query statements, verify