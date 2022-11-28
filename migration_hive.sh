#/bin/bash

# step1.load local FUNC
. migration_common.sh &> /dev/null
[ $? -ne 0 ] && { echo -e "\033[31m [ERROR] $(date +%Y-%m-%d" "%H:%M:%S) Parsing error, line:${LINENO} file does not exist migration_common.sh \033[0m"; exit 1; }

# step2.create logdir
log_date=$(date "+%F")
[ -d "migration_logs" ] && info "migration_logs has created"; touch "migration_logs/migration_log.$log_date" || { info "can not find dir migration_logs,create migration_logs";mkdir "migration_logs" && touch "migration_logs/migration_log.$log_date" && info "create migration_logs Success"; }

if [[ $1 == "-h" || $1 == "-H" || $1 == "--help" ]];then
   print_usage
   exit 1;
fi

# step3.start
[ "$#" -eq 1 ] && info "[Main] start migration hive ..." || { err_info "${LINENO} [Main] Invalid parameter '\'[$*]'\'";exit 1; }

mode=$1
[ -z $mode ] && { err_info "${LINENO} [Main] Invalid parameter '\'$mode'\'";exit 1; }

# step4.load env
load_env "/etc/profile"
load_env "migration_env.sh"

# step5.check hadoop env
HADOOP_HOME=${HADOOP_HOME:-$HADOOP_HOME}
HBASE_HOME=${HIVE_HOME:-$HIVE_HOME}
checkParameter "HADOOP_HOME"
checkParameter "HIVE_HOME"
checkParameter "sourceHiveConfigPath"
checkParameter "targetHiveConfigPath"

# step6.check table file
checkMigrationTableFile
if [ $? -eq 0 ];then
    generateTableFile
else
    [ -f ${migrationTableFile} ] && info "[Main] Table file ${migrationTableFile} has created" || { err_info "${LINENO} [Main] Table file is not created";exit 1; }
fi

# step7.set threads
DEFAULT_THREADS=5
threadTask=${threadTask:-$DEFAULT_THREADS}

(expr $threadTask + 0 &>/dev/null) && info "[Main] threadTask set to $threadTask" || err_info "${LINENO}  [Main] Invalid parameter '\'$threadTask'\'"
if [ ${threadTask} -ge 50 ];then
   err_info "${LINENO} [Main] The number of applied threads is ${threadTask} greater than 50"
   exit 1;
fi

# step8.check current user
cur_user=$(whoami)

info "[Main] The current user is ${cur_user}"
if [ ${cur_user} != ${hive_user} ];then
    err_info "${LINENO} [Main] The configured user is ${hive_user} inconsistent with the execution user ${cur_user}"
    exit 1;
fi

# step9.core
# verify migrate all
case $mode in
    # 导出建表语句
    "exportCreateTableSql" | "crate")
        info "[Main] mode is export create table sql..."
        enableMultiThread ${threadTask}
        generateCreateHiveTable
    ;;
    "migrateTableData" | "migrate")
        info "[Main] mode is migrateTableData..."
        # enableMultiThread ${threadTask}
        # generateHiveTablePath
        # TODO
        enableMultiThread ${threadTask}
        migrateHiveData
        # TODO
        msckRepairHiveData
    ;;
    # 生成查询语句
    "createQuerySql" | "query")
        info "[Main] mode is createQuerySql..."
        enableMultiThread ${threadTask}
        generateQueryHiveTable
    ;;
    "verifyTableData" | "verify")
        info "[Main] mode is verifyTableData..."
        [ -f ${hiveQuerySqlFile} ] && info "[Main] Table file $hiveQuerySqlFile} has created" || { err_info "${LINENO} [Main] Table file is not created";exit 1; }
    ;;
    "all")
        info "[Main] mode is verify and migrate..."
    ;;

    *)
        err_info "${LINENO} [Main]  Invalid parameter '\'${mode}'\'"
        exit 1
esac