#!/bin/bash
# [general config]
export hive_user=hbase
export sourceHiveConfigPath=/etc/hive-server
export targetHiveConfigPath=/etc/hive-server
export threadTask=5
# ${sourceDB},${sourceTable},${targetDB},${targetTable}
export migrationTableFile=table.txt

export autoCreateTableFile=Y
export sourceDatabase=tpcds_bin_partitioned_orc_10
export targetDatabase=tpcds_bin_partitioned_orc_10


# [ createTableSql ]
export tableDDL=tablesDDL.txt

# [ createQuerySql ]
export hiveQuerySqlFile=hiveQuerySqlFile.txt

# [verify config]
# fullTableScan/partitionScan
export verifyMode=fullTableScan
# 默认分区格式都相同
export verifyStartPartition=
export verifyEndPartition=2451620

# [migrateTableData config]
export sourceDistcpHiveRPCAddress=hdfs://ns-fed
export sourceHiveHdfsPath=/user/hive/warehouse
export targetDistcpHiveRPCAddress=hdfs://ns-fed
export targetHiveHdfsPath=/tmp/hivemig
export distcpMaps=
export distcpBandwidth=
# DEFAULT_DIR="partitionInfo"
export partitionInfoDir=partitionInfo