#!/bin/bash
base_path=$(cd `dirname $0`; pwd)
cd ${base_path}

job_name=$1
latest_job_state=$2

if [ ! -n "${job_name}" ]; then
  echo "please assign job name";exit 1;
fi

if echo "$job_name" | grep -q -E '\.sql$'
then
  job_name=`echo ${job_name%.sql}`
fi

export CUSTOME_CLASSPATHS=${base_path}/template
export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} -Dtemplate.default=env_local "

hdfs_base_dir='/user/flink/checkpoints'
hdfs_host='hdfs://default'
flink_job_command='/usr/flink/flink-1.17.1/bin/flink-local run'

kill_all_application_by_name_on_yarn(){
  job_name=$1
  application_ids=$(yarn application -list|grep "${job_name}.sql"|awk -F "application_" '{print $2}'|awk '{print "application_"$1"!!!!"$2}')
  for line in ${application_ids}
  do
    application_info=(${line//!!!!/ })
    application_id=${application_info[0]}
    application_name=${application_info[1]}
    if echo "$application_name" | grep -q -E '\.sql$'
    then
      application_name=`echo ${application_name%.sql}`
    fi
    if [ "$application_name" == "$job_name" ];then
      if [[ -z "${NO_PROMPT}" ]]; then
          read -r -p "Find running application on yarn, Application Id is ${application_id}, Application Name is ${application_name}, Do you want to kill it? [Y/n]" input
          case $input in
              [yY][eE][sS]|[yY])
                  yarn application -kill ${application_id}
                  ;;
          esac
      else #
          #default to kill
          yarn application -kill ${application_id}
      fi
    fi
  done
}

get_latest_job_id_by_name(){
  job_name=$1
  hdfs_base_dir=$2
  hadoop fs -ls ${hdfs_base_dir}/"${job_name}"/ \
    |awk -F "hdfs          0" '{print $2}'    \
    |sort -n -r                               \
    |awk -F "${job_name}/"  'NR==1{print $2}'  \
    |awk '{if (length == 32) print $0}'
}

get_latest_job_state(){
  job_name=$1
  hdfs_base_dir=$2
  job_id=$3
  if [ "${job_id}" = "" ]
  then
    echo ""
  else
    all_state_size=$(hadoop fs -ls "${hdfs_base_dir}/${job_name}/${job_id}/" \
                        |awk -F "hdfs          0" '{print $2}' |sort -n -r|awk '{if($0 ~ /chk-/)print $3}'|wc -l)
    if [ "$all_state_size" != 1 ];
    then
      child_list=$(hadoop fs -ls "${hdfs_base_dir}/${job_name}/${job_id}/" \
              |awk -F "hdfs          0" '{print $2}' |sort -n -r|awk '{if($0 ~ /chk-/)print $3}')
      result_chk=""
      for chk in $child_list
      do
        # shellcheck disable=SC2126
        has_meta=$(hadoop fs -ls "$chk"|grep "_meta"|wc -l)
        if [ "$has_meta" == 1 ];
        then
           result_chk=$chk
           break
        fi
      done
      echo "$result_chk"
    else
      hadoop fs -ls "${hdfs_base_dir}/${job_name}/${job_id}/" \
        |awk -F "hdfs          0" '{print $2}' |sort -n -r|awk '{if($0 ~ /chk-/)print $3}'
    fi
  fi
}

get_start_command_in_script(){
  job_name=$1
  cat "${job_name}.sql" |while read line
  do
    result=$(echo ${line} | grep "/usr/flink")
    if [[ "$result" != "" ]]; then
      echo ${result/--/};break
    fi
  done
}

start_job(){
  job_name=$1
  latest_job_state=$2
  script_command=$(get_start_command_in_script ${job_name})
  array=(${script_command//run-application/ })
  command_params=""
  for var in ${array[@]}
  do
    is_first=$(echo $var | grep "/usr/flink")
    if [[ "${is_first}" != "" ]]
    then
      command_params="${flink_job_command} "
      if [ "${latest_job_state}" != "" ]; then
        if [[ -z "${NO_PROMPT}" ]]; then
            read -r -p "Do you want to start the job from state ${latest_job_state}? [Y/n]" yes_no
            case ${yes_no} in
                [yY][eE][sS]|[yY])
                    command_params="${command_params} -s ${hdfs_host}${latest_job_state}  "
                    ;;
            esac
        else
            if ! [[ -z "${RESTORE_STATE}" ]]; then
              command_params="${command_params} -s ${hdfs_host}${latest_job_state}  "
            fi
        fi
      fi
      command_params="${command_params} -t yarn-per-job -c name.zicat.astatine.sql.client.SqlClient -Dstate.checkpoints.dir=${hdfs_host}${hdfs_base_dir}/${job_name} -Dpipeline.name=${job_name} -Dyarn.application.name=${job_name}.sql -Dhigh-availability.zookeeper.path.root=/flink/${job_name} -Dsecurity.kerberos.token.provider.hadoopfs.renewer=yarn "
    else
      command_params="$command_params $var "
    fi
  done
  real_command="${command_params} astatine-sql-client-1.0-SNAPSHOT.jar ${job_name}.sql"
  echo "start command = ${real_command}"
  bash ${real_command}
}

# kill all running job on yarn
kill_all_application_by_name_on_yarn ${job_name}
# get latest job id by job name
job_id=$(get_latest_job_id_by_name $job_name $hdfs_base_dir)
# get latest job state
if [ ! -n "${latest_job_state}" ]; then
  latest_job_state=$(get_latest_job_state $job_name $hdfs_base_dir ${job_id})
fi
# start new job with state
start_job $job_name $latest_job_state