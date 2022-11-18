#!/bin/bash
usage="Usage: migration_hive.sh -h "

load_env(){

   [ -f "$1" ] && . "$1" &>/dev/null || { err_info "${LINENO} can not find file $1";exit 1; }

}

err_info(){

   echo -e "\033[31m [ERROR] $(date +%Y-%m-%d" "%H:%M:%S) Parsing error, line:$1  please check, $usage \033[0m"
   echo -e "[ERROR] $(date +%Y-%m-%d" "%H:%M:%S) Parsing error, line:$1  please check, $usage" >> "migration_logs/migration_log.$log_date"

}

info(){

   echo -e "\033[32m [INFO] $(date +%Y-%m-%d" "%H:%M:%S) $1 \033[0m"
   echo -e "[INFO] $(date +%Y-%m-%d" "%H:%M:%S) $1" >> "migration_logs/migration_log.$log_date"

}

print_usage(){

   echo -e "Usage: bash \${mig_home}/migration_hive.sh verify                   \n # verify hive data"
   echo -e "Usage: bash \${mig_home}/migration_hive.sh migration                \n # migration hive data"

}

checkParameter(){

   config=${1}
   res=${!config}
   [ -n "${res}" ] && info "[checkParameter] $1 set to ${res}"|| { err_info "${LINENO} [checkParameter] The parameter $1 cannot be empty"; exit 1; }

}

checkParameterIsEmpty(){
    parm=${1}
    [ -z "${parm}" ] && { err_info "${LINENO} [checkParameterIsEmpty] The parameter cannot be empty"; exit 1; }
}

checkHdfsDirExists(){

   $HADOOP_HOME/bin/hdfs dfs -test -d $1
   if [ $? -eq 0 ];then
      info "[checkHdfsDirExists] $1 The directory already exists"
      return 0
   else
      info "[checkHdfsDirExists] $1 The directory not exists"
      return 1
   fi

}

generateTableFile(){
    checkParameter "sourceDatabase"
    info "start generate table file please wait..."
    hive --config ${sourceHiveConfigPath} -e "use ${sourceDatabase}; show tables;" 1>${migrationTableFile} 2>/dev/null
    [ $? -ne 0 ] && { err_info "${LINENO} [generateTableFile] Unable to generate table file. Please check ";exit 1;  }
    sed -i "s/\(.*\)/${sourceDatabase},\1,${targetDatabase},\1/g" ${migrationTableFile}

    info "generate table file successed,table file is ${migrationTableFile}"
}

splitLine(){
    common=$1
    sourceDB=$(echo $line | cut -d "," -f1)
    sourceTable=$(echo $line | cut -d "," -f2)
    targetDB=$(echo $line | cut -d "," -f3)
    targetTable=$(echo $line | cut -d "," -f4)
    checkParameterIsEmpty ${sourceDB}
    checkParameterIsEmpty ${sourceTable}
    checkParameterIsEmpty ${targetDB}
    checkParameterIsEmpty ${targetTable}
    checkParameterIsEmpty ${hiveQuerySqlFile}
}

checkTableExistPartition(){
    checkDB=$1
    checkTable=$2
    hive --config ${sourceHiveConfigPath} -e "show partitions ${checkDB}.${checkTable};" >/dev/null 2>&1
    if [ $? -ne 0 ];then
      return 1
    else
      export partitionKey=$(hive --config ${sourceHiveConfigPath} -e "show  partitions ${checkDB}.${checkTable};" 2>/dev/null | head -1 | cut -d "=" -f 1)
      return 0
    fi
}

checkTableExistPartitionAndAppendfile(){
    checkDB="$1.db"
    checkTable=$2
    escapeSourceDistcpHiveRPCAddress=$3
    escapeSourceHiveHdfsPath=$4
    hive --config ${sourceHiveConfigPath} -e "show  partitions ${checkDB}.${checkTable};" >/dev/null 2>&1
    if [ $? -ne 0 ];then
      echo -e "${sourceDistcpHiveRPCAddress}/${sourceHiveHdfsPath}/${checkDB}/${checkTable}" > ${partitionInfoDir}/${checkDB}.${checkTable}
      return 1
    else
      hive --config ${sourceHiveConfigPath} -e "show  partitions ${checkDB}.${checkTable};" 1>${partitionInfoDir}/${checkDB}.${checkTable} 2>/dev/null
      sed -i "s/^/${escapeSourceDistcpHiveRPCAddress}${escapeSourceHiveHdfsPath}\/${checkDB}\/${checkTable}\//g" ${partitionInfoDir}/${checkDB}.${checkTable}
      return 0
    fi    
}

generateCreateHiveTable(){
    count=0
    cat /dev/null > ${tableDDL}
    for line in $(cat ${migrationTableFile});do
        (( count++ ))
        read -u9
        {
            splitLine ${line}
            hive --config ${sourceHiveConfigPath} -e "show create table ${sourceDB}.${sourceTable};" 1>>${tableDDL} 2>/dev/null
            echo -e ";\n" >> ${tableDDL}
            echo "" >&9
        }&
    done
    wait
    # close the pipe
    exec 9>&-
}

checkMigrationTableFile(){
    checkParameter "autoCreateTableFile"
    case ${autoCreateTableFile} in
    "Y"|"y"|"yes"|"YES")
        return 0
    ;;
    "N"|"n"|"no"|"NO")
        return 1
    ;;
    *)
        err_info "${LINENO} [checkMigrationTableFile] Invalid parameter '\'${autoCreateTableFile}'\'" && exit 1;
    ;;
    esac
}

generateHiveTablePath(){

    checkParameter "sourceDistcpHiveRPCAddress"
    checkParameter "sourceHiveHdfsPath"
    checkParameter "targetDistcpHiveRPCAddress"
    checkParameter "targetHiveHdfsPath"
    DEFAULT_DIR="partitionInfo"
    partitionInfoDir=${partitionInfoDir:-$DEFAULT_DIR}
    [ -d ${partitionInfoDir} ] && { info "${partitionInfoDir} directory has created" ;} || { info "${partitionInfoDir} directory does not exist, create directory" ; mkdir -p "${partitionInfoDir}" ;}
    [ $? -eq 0 ] && { info "create directory ${partitionInfoDir} sucessed"; } || { err_info "create directory ${partitionInfoDir} failed"; exit 1 ; }

    count=0
    for line in $(cat ${migrationTableFile});do
        (( count++ ))
        read -u9
        {
            splitLine ${line}
            # "s/^/\/test\//g"
            escapeSourceDistcpHiveRPCAddress=${sourceDistcpHiveRPCAddress//\//\\\/}
            escapeSourceHiveHdfsPath=${sourceHiveHdfsPath//\//\\\/}
            info "[checkTableExistPartitionAndAppendfile-$count] start dealing with $sourceDB:$sourceTable"
            checkTableExistPartitionAndAppendfile ${sourceDB} ${sourceTable} ${escapeSourceDistcpHiveRPCAddress} ${escapeSourceHiveHdfsPath}
            info "[checkTableExistPartitionAndAppendfile-$count] finish dealing with $sourceDB:$sourceTable"
            echo "" >&9
        
        }&
        done
        wait
        # close the pipe
        exec 9>&-
    # checkHdfsDirExists ${targetDistcpHiveRPCAddress}${targetHiveHdfsPath}
}

generateQueryHiveTable(){
    
    checkParameter "verifyMode"
    case ${verifyMode} in
    "fullTableScan")
      [ -z "${verifyStartPartition}" ] && { info "verifyStartPartition is empty, Start scanning from the first row"; export verifyStartPartition="null"; } || { info "verifyStartPartition is ${verifyStartPartition}"; }
      [ -z "${verifyEndPartition}" ] && { info "verifyEndPartition is empty, Will scan until the last line"; export verifyEndPartition="null"; } || { info "verifyEndPartition is ${verifyEndPartition}"; }
      [ -f ${hiveQuerySqlFile} ] && { info "hiveQuerySqlFile ${hiveQuerySqlFile} has created"; } || { info "create hiveQuerySqlFile ${hiveQuerySqlFile}" ; touch ${hiveQuerySqlFile} ; }
      [ $? -ne 0 ] && { err_info "${LINENO} [generateQueryHiveTable] Unable to create table file. Please check "; exit 1; }
      info "start generateQueryHiveTable"
      cat /dev/null > ${hiveQuerySqlFile}
      # DB table partition
      if [[ ${verifyStartPartition} == "null" && ${verifyEndPartition} == "null" ]]; then
        count=0
        for line in $(cat ${migrationTableFile});do
            (( count++ ))
            read -u9
            {
                splitLine ${line}
                echo -e "select count(1) from ${sourceDB}.${sourceTable};|select count(1) from ${targetDB}.${targetTable};" >> ${hiveQuerySqlFile}
                echo "" >&9
            }&
        done
        wait
        # close the pipe
        exec 9>&-
      elif [[ ${verifyStartPartition} == "null" && ${verifyEndPartition} != "null" ]]; then
        count=0
        for line in $(cat ${migrationTableFile});do
            (( count++ ))
            read -u9
            {
                splitLine ${line}
                checkTableExistPartition ${sourceDB} ${sourceTable}
                if [ $? -ne 0 ]; then
                echo -e "select count(1) from ${sourceDB}.${sourceTable} ;|select count(1) from ${targetDB}.${targetTable};" >> ${hiveQuerySqlFile}
                echo "" >&9
                else
                echo -e "select count(1) from ${sourceDB}.${sourceTable} where ${partitionKey} <= ${verifyEndPartition};|select count(1) from ${targetDB}.${targetTable} where ${partitionKey} <= ${verifyEndPartition};" >> ${hiveQuerySqlFile}
                echo "" >&9
                fi
            }&
        done
        wait
        # close the pipe
        exec 9>&-
      elif [[ ${verifyStartPartition} != "null" && ${verifyEndPartition} == "null" ]]; then
        count=0
        for line in $(cat ${migrationTableFile});do
            (( count++ ))
            read -u9
            {
                splitLine ${line}
                checkTableExistPartition ${sourceDB} ${sourceTable}
                if [ $? -ne 0 ]; then
                echo -e "select count(1) from ${sourceDB}.${sourceTable} ;|select count(1) from ${targetDB}.${targetTable};" >> ${hiveQuerySqlFile}
                echo "" >&9
                else
                echo -e "select count(1) from ${sourceDB}.${sourceTable} where ${partitionKey} >= ${verifyStartPartition};|select count(1) from ${targetDB}.${targetTable} where ${partitionKey} >= ${verifyStartPartition};" >> ${hiveQuerySqlFile}
                echo "" >&9
                fi
            }&
        done
        wait
        # close the pipe
        exec 9>&-
      elif [[ ${verifyStartPartition} != "null" && ${verifyEndPartition} != "null" ]]; then
        count=0
        for line in $(cat ${migrationTableFile});do
            (( count++ ))
            read -u9
            {
                splitLine ${line}
                checkTableExistPartition ${sourceDB} ${sourceTable}
                if [ $? -ne 0 ]; then
                echo -e "select count(1) from ${sourceDB}.${sourceTable} ;|select count(1) from ${targetDB}.${targetTable};" >> ${hiveQuerySqlFile}
                echo "" >&9
                else
                echo -e "select count(1) from ${sourceDB}.${sourceTable} where ${partitionKey} >= ${verifyStartPartition} and ${partitionKey} <= ${verifyStartPartition};|select count(1) from ${targetDB}.${targetTable} where ${partitionKey} >= ${verifyStartPartition} and ${partitionKey} <= ${verifyStartPartition};" >> ${hiveQuerySqlFile}
                echo "" >&9
                fi
            }&
        done
        wait
        # close the pipe
        exec 9>&-
      fi
      info "generateQueryHiveTable finshed"
    ;;
    "partitionScan")
      info "TODO...partitionScan"
    ;;
    *)
      err_info "${LINENO} [startMultiThread] Invalid parameter '\'$1'\'" && exit 1;
   esac

}

# queryHiveTable(){

  
# }

enableMultiThread(){

   fifoFile="migration_fifo"
   rm -f ${fifoFile}
   mkfifo ${fifoFile}
   [ $? -ne 0 ] && { err_info "${LINENO} [enableMultiThread] Unable to create FIFO file. Please check permissions";exit 1; }
   # create fileDescriptor
   exec 9<> ${fifoFile}
   rm -f ${fifoFile}
   for ((i=0;i<$1;i++))
   do
      echo "" >&9
   done
   info "[enableMultiThread] wait all task finish,then exit!!!"

}

migrateHiveData(){
    # TODO 遍历迁移的文件，取出 DB.TB 去hive 分区目录下拷贝对应表的path
    echo -e "migrateHiveData"
}

msckRepairHiveData(){
    # 执行 msck repair 修复table
    echo -e "msckRepairHiveData"
}

startMultiThread(){
   case $1 in
      "verify")
         count=0
         for line in $(cat ${migrationTableFile});do
            (( count++ ))
            read -u9
            {
               verifyModeStatus=$2
               info "[Thread-$count] start query with ${verifyModeStatus} ${sourceDB}:${sourceTable}"
            #    queryHiveTable ${verifyModeStatus} ${sourceDB} ${sourceTable}
               info "[Thread-$count] finish query with ${verifyModeStatus} ${sourceDB}:${sourceTable}"

               info "[Thread-$count] start query with ${verifyModeStatus} ${targetDB}:${targetTable}"
            #    queryHiveTable ${verifyModeStatus} ${sourceDB} ${sourceTable}
               info "[Thread-$count] finish query with ${verifyModeStatus} ${targetDB}:${targetTable}"
               echo "" >&9
            }&
         done
         wait
         # close the pipe
         exec 9>&-
         ;;
      "migrate")
         count=0
         for line in $(cat ${migrationTableFile});do
            (( count++ ))
            read -u9
            {
               sourceTable=$(echo $line | cut -d "," -f1)
               targetTable=$(echo $line | cut -d "," -f2)
               sourceTable_snapshot="${sourceTable}_snapshot"
               targetTable_snapshot="${targetTable}_snapshot"
               checkParameterIsEmpty ${sourceTable}
               checkParameterIsEmpty ${targetTable}
               info "[startMultiThread-$count] start dealing snapshot with ${sourceTable_snapshot} to ${targetTable_snapshot}"
               snapshotHbaseTable "${sourceTable_snapshot}" "${targetTable_snapshot}"
               info "[startMultiThread-$count] finish dealing snapshot with ${sourceTable_snapshot} to ${targetTable_snapshot}"
               echo "" >&9
            }&
         done
         wait
         # close the pipe
         exec 9>&-
         ;;
      *)
         err_info "${LINENO} [startMultiThread] Invalid parameter '\'$1'\'" && exit 1;
   esac
}