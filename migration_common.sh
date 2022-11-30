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
    checkDB="$1"
    checkTable=$2
    escapeSourceDistcpHiveRPCAddress=$3
    escapeSourceHiveHdfsPath=$4
    escapeTargetDistcpHiveRPCAddress=$5
    escapeTargetHiveHdfsPath=$6
    hive --config ${sourceHiveConfigPath} -e "show  partitions ${checkDB}.${checkTable};" >/dev/null 2>&1
    # for test 测试
    if [ $? -ne 0 ];then
      echo -e "${sourceDistcpHiveRPCAddress}${sourceHiveHdfsPath}/${checkDB}.db/${checkTable},${targetDistcpHiveRPCAddress}${targetHiveHdfsPath}/${checkDB}.db/${checkTable}" > ${partitionInfoDir}/${checkDB}.${checkTable}
      return 1
    else
      hive --config ${sourceHiveConfigPath} -e "show  partitions ${checkDB}.${checkTable};" 1>${partitionInfoDir}/${checkDB}.${checkTable} 2>/dev/null
    #   sed -i "s/^/${escapeSourceDistcpHiveRPCAddress}${escapeSourceHiveHdfsPath}\/${checkDB}.db\/${checkTable}\//g" ${partitionInfoDir}/${checkDB}.${checkTable}
      sed -i "s/\(.*\)/${escapeSourceDistcpHiveRPCAddress}${escapeSourceHiveHdfsPath}\/${checkDB}.db\/${checkTable}\/\1,${escapeTargetDistcpHiveRPCAddress}${escapeTargetHiveHdfsPath}\/${checkDB}.db\/${checkTable}\/\1/g" ${partitionInfoDir}/${checkDB}.${checkTable}
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
            escapeTargetDistcpHiveRPCAddress=${targetDistcpHiveRPCAddress//\//\\\/}
            escapeTargetHiveHdfsPath=${targetHiveHdfsPath//\//\\\/}
            info "[checkTableExistPartitionAndAppendfile-$count] start dealing with $sourceDB:$sourceTable"
            checkTableExistPartitionAndAppendfile ${sourceDB} ${sourceTable} ${escapeSourceDistcpHiveRPCAddress} ${escapeSourceHiveHdfsPath} ${escapeTargetDistcpHiveRPCAddress} ${escapeTargetHiveHdfsPath}
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
    # set distcp
    DEFAULT_MAPS=5
    DEFAULT_Bandwidth=20
    distcpMaps=${distcpMaps:-$DEFAULT_MAPS}
    distcpBandwidth=${distcpBandwidth:-$DEFAULT_Bandwidth}
    (expr $distcpMaps + $distcpBandwidth &>/dev/null) && info "[migrateHiveData] distcpMaps set to $distcpMaps , distcpBandwidth set to $distcpBandwidth" || err_info "${LINENO} [migrateHiveData] Invalid parameter '\'$distcpMaps'\' or '\'$distcpBandwidth'\'"
    # check
    if [ ${distcpBandwidth} -ge 50 ];then
        err_info "${LINENO} [migrateHiveData] The distcpBandwidth of applied threads is ${distcpBandwidth} greater than 50"
        exit 1;
    fi

    if [ ${distcpMaps} -ge 100 ];then
        err_info "${LINENO} [migrateHiveData] The distcpMaps of applied threads is ${distcpMaps} greater than 100"
        exit 1;
    fi

    for line in $(cat ${migrationTableFile});do
        splitLine ${line}
        # 测试
        checkHdfsDirAndBackup "${targetDistcpHiveRPCAddress}${targetHiveHdfsPath}/${sourceDB}.db/${sourceTable}"
        count=0
        for tablePath in $(cat ${partitionInfoDir}/${sourceDB}.${sourceTable});do
            (( count++ ))
            read -u9
            {
                sourceHiveHdfsTablePath=$(echo $tablePath | cut -d "," -f1)
                targetHiveHdfsTablePath=$(echo $tablePath | cut -d "," -f2)
                info "[migrateHiveData-$count] start dealing with $sourceDB:$sourceTable distcp path is $sourceHiveHdfsTablePath to $targetHiveHdfsTablePath"
                distcpHdfsFile ${sourceHiveHdfsTablePath} ${targetHiveHdfsTablePath}
                info "[migrateHiveData-$count] finish dealing with $sourceDB:$sourceTable distcp path is $sourceHiveHdfsTablePath to $targetHiveHdfsTablePath"
                echo "" >&9
            }&
        done
        #wait命令的意思是，等待（wait命令）上面的命令（放入后台的）都执行完毕了再往下执行。
        wait
    done
    # close the pipe
    exec 9>&- 

}

checkHdfsDirAndBackup(){
    backDate=$(date "+%Y-%m-%d_%Hhr_%Mmi_%Sse")
    backName="$1.${backDate}.$(whoami).bak"
    $HADOOP_HOME/bin/hdfs dfs -test -d $1 > /dev/null 2>&1
    if [ $? -eq 0 ];then
        info "$1 The directory already exists. "
        $HADOOP_HOME/bin/hdfs dfs -mv $1 ${backName} > /dev/null 2>&1
        [ $? -eq 0 ] && info "[checkHdfsDirAndBackup] $1 backup success,bakcup dir is ${backName}" || { err_info "${LINENO} [checkHdfsDirAndBackup] Unable to $1 backup ${backName}.Please check"; exit 1; }
    else
        info "$1 The directory not exists continue distcp."
    fi
}

distcpHdfsFile(){

    if [ $# -eq 2 ];then
        sourceHiveHdfsTablePath=$1
        targetHiveHdfsTablePath=$2
        [ -z ${sourceHiveHdfsTablePath} ] && { err_info "${LINENO} [distcpHdfsFile] The source hive hdfs path cannot be empty"; exit 1; }
        [ -z ${targetHiveHdfsTablePath} ] && { err_info "${LINENO} [distcpHdfsFile] The target hive hdfs path cannot be empty"; exit 1; }
        hadoop distcp -Dmapreduce.job.name="distcp ${sourceDB}:${sourceTable}" -pugpb -m "$distcpMaps" -bandwidth "$distcpBandwidth" -overwrite ${sourceHiveHdfsTablePath}  ${targetHiveHdfsTablePath} >> "migration_logs/${sourceTable}_distcp.log" 2>&1
   else
      err_info "${LINENO} [distcpHdfsFile] Invalid parameter '\'$1'\'" && exit 1;
   fi

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