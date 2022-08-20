#!/bin/bash

# chkconfig: 3 90 10
# description: obproxyd is a daemon which guarantee obproxy is running

OBPROXY_CMD=
OBPROXY_APP_NAME=${APPNAME}
OBPROXY_APP_NAME_ARG=
OBPROXY_IDC_NAME=${OBPROXY_IDC_NAME}
OBPROXY_IDC_NAME_ARG=
OBPROXY_ROOT=$(dirname $(dirname $(readlink -f "$0")))
OBPROXY_PORT=${OBPROXY_PORT}
ENABLE_ROOT_SERVER=${ENABLE_ROOT_SERVER}
ENABLE_ROOT_SERVER_ARG=
ROOT_SERVER_CLUSTER_NAME=${ROOT_SERVER_CLUSTER_NAME}
ROOT_SERVER_CLUSTER_NAME_ARG=
ROOT_SERVER_LIST=${ROOT_SERVER_LIST}
ROOT_SERVER_LIST_ARG=
OBPROXY_CONFIG_SERVER_URL=${OBPROXY_CONFIG_SERVER_URL}
OBPROXY_CONFIG_SERVER_URL_ARG=

STATUS_ALL_OBPROXY=

function success_msg()
{
    echo -e "\033[32m $1 \033[0m"
}

function fail_msg()
{
    echo -e "\033[31m $1 \033[0m"
}

function info_msg()
{
    echo -e "\033[35m $1 \033[0m"
}

function help()
{
    success_msg "USAGE:`basename $0` -c COMMAND [-n APP_NAME] [-i IDC_NAME]"
    success_msg "    The commands(-c) are:"
    success_msg "      start"
    success_msg "      stop"
    success_msg "      status"
    success_msg "      checkalive"
    success_msg ""
    success_msg "e.g."
    success_msg "1. ./bin/obproxyd.sh -c start -n obpay -i idc1"
}

function check_opt()
{
    # check COMMAND
    case $OBPROXY_CMD in
        start)
            ;;
        stop)
            ;;
        status)
            ;;
        checkalive)
            ;;
        *)
            if [ -z "$OBPROXY_CMD" ];
            then
                fail_msg "COMMAND is empty"
            else
                fail_msg "COMMAND not support: $OBPROXY_CMD"
            fi

            help
            exit 255
            ;;
    esac

    if [ x$ENABLE_ROOT_SERVER == xtrue ];then
      ENABLE_ROOT_SERVER_ARG=" -r"
      if [ -z $ROOT_SERVER_CLUSTER_NAME ]
      then
        echo "ROOT_SERVER_CLUSTER_NAME not set"
        exit 1
      else
        ROOT_SERVER_CLUSTER_NAME_ARG=" -s ${ROOT_SERVER_CLUSTER_NAME}"
      fi

      if [ -z $ROOT_SERVER_LIST ]
      then
        echo "ROOT_SERVER_LIST not set"
        exit 1
      else
        ROOT_SERVER_LIST_ARG=" -t ${ROOT_SERVER_LIST}"
      fi
    else
      echo "obproxy config server url:$OBPROXY_CONFIG_SERVER_URL"
      OBPROXY_CONFIG_SERVER_URL_ARG=" -u $OBPROXY_CONFIG_SERVER_URL"
    if

    # check PORT
    if [ -z $OBPROXY_PORT ]
    then
      if [ ! -z $obproxy_port ]
      then
        OBPROXY_PORT=$obproxy_port
      else
        echo "env OBPROXY_PORT not set, use default 2883"
        OBPROXY_PORT=2883
      fi
    fi
    info_msg "port is: $OBPROXY_PORT"

    # check APP_NAME
    if [ -z "$OBPROXY_APP_NAME" ];
    then
        info_msg "unknown app name"
    else
        info_msg "app name is: $OBPROXY_APP_NAME"
        OBPROXY_APP_NAME_ARG=" -n ${OBPROXY_APP_NAME}"
    fi

    # check IDC_NAME
    if [ -z "$OBPROXY_IDC_NAME" ];
    then
        info_msg "unknown idc name"
    else
        info_msg "idc name is: $OBPROXY_IDC_NAME"
        OBPROXY_IDC_NAME_ARG=" -i ${OBPROXY_IDC_NAME}"
        OBPROXY_OPT_LOCAL="${OBPROXY_OPT_LOCAL},proxy_idc_name=$OBPROXY_IDC_NAME"
    fi

    if [ -z $WORK_THREAD_NUM ]
    then
      echo "env WORK_THREAD_NUM not set, use default 8"
      WORK_THREAD_NUM=8
    fi
    echo "obproxy work_thread_num:$WORK_THREAD_NUM"

    if [ ! -z $NEED_CONVERT_VIP_TO_NAME ]
    then
      echo "obproxy NEED_CONVERT_VIP_TO_NAME: $NEED_CONVERT_VIP_TO_NAME"
      OBPROXY_OPT_LOCAL="${OBPROXY_OPT_LOCAL},need_convert_vip_to_tname=true"
    fi

    if [ ! -z $OBPROXY_EXTRA_OPT ]
    then
      echo "obproxy OBPROXY_EXTRA_OPT: $OBPROXY_EXTRA_OPT"
      OBPROXY_OPT_LOCAL="${OBPROXY_OPT_LOCAL},$OBPROXY_EXTRA_OPT"
    fi

    OBPROXY_OPT_LOCAL=",enable_cached_server=true,enable_get_rslist_remote=true,monitor_stat_dump_interval=1s,enable_qos=true,enable_standby=false,query_digest_time_threshold=2ms,monitor_cost_ms_unit=true,enable_strict_kernel_release=false,enable_proxy_scramble=true,work_thread_num=$WORK_THREAD_NUM,proxy_mem_limited='2G',log_dir_size_threshold=10G${OBPROXY_OPT_LOCAL}"
}

# change to the path where this script locates.
cd "$(dirname "$(readlink -f "$0")")/.."

# Stop and status do not need to check the directory permissions, to prevent the business side
# from using the log account to checkservice to fail the permission check
function check_dir()
{
  if [ ! -f $OBPROXY_ROOT/bin/obproxy ]
  then
    fail_msg "no $OBPROXY_ROOT/bin/obproxy file exist, please check it"
    exit 1
  fi

  # If the directory does not exist, just create a soft link, and the directory will be created when OBProxy starts
  if [ ! -e $OBPROXY_ROOT/log ]
  then
    ln -fs /home/admin/logs/obproxy/log $OBPROXY_ROOT/log
  elif [ ! -w $OBPROXY_ROOT/log ]
  then
    fail_msg "no $OBPROXY_ROOT/log dir exist or permission denied, please check it"
    exit 1
  fi
}

function start()
{
    is_checkalive_exists
    if [ $? -ge 1 ]
    then
        success_msg "obproxy already started"
        exit 0
    fi

    (cd $OBPROXY_ROOT; nohup ./bin/obproxyd.sh -c checkalive -p $OBPROXY_PORT $OBPROXY_APP_NAME_ARG $OBPROXY_IDC_NAME_ARG $ENABLE_ROOT_SERVER_ARG $ROOT_SERVER_LIST_ARG $ROOT_SERVER_CLUSTER_NAME_ARG $OBPROXY_CONFIG_SERVER_URL_ARG > /dev/null 2>&1 &)

    success_msg "obproxy started"
}

function stop()
{
    # The obproxyd script cannot kill all directly, because the stop command is also the obproxyd.sh script
    is_checkalive_exists
    if [ $? -ge 1 ]
    then
        kill_all_checkalive
    fi
    is_obproxy_exists
    if [ $? -ge 1 ]
    then
        kill_obproxy 15
    fi

    # If you do not exit for a long time, just kill -9
    is_obproxy_exists
    if [ $? -ge 1 ]
    then
        i=3
        while [ $i -gt 0 ]
        do
            sleep 1
            is_obproxy_exists
            if [ $? -eq 0 ]
            then
                break;
            fi
            i=$[$i-1]
        done

        if [ $i -eq 0 ]
        then
            kill_obproxy 9
        fi
    fi

    success_msg "obproxy stopped"
}

function status()
{
    # Maybe the user uses the log account to checkservice,
    # so here it directly checks whether the obproxy of all accounts exists
    is_obproxy_exists
    if [ $process_count -ge 1 ];
    then
        echo "ObProxy is running ..."
    else
        echo "ObProxy is stopped"
    fi
}

function checkalive()
{
    is_first=true;
    while [ 1 ]
    do
        restart=0
        is_obproxy_exists
        if [ $? -ge 1 ]
        then
            is_obproxy_exists
            if [ $? -ge 1 ]
            then
                is_obproxy_healthy
                if [ $? -ne 0 ]
                then
                    restart=1
                    echo "ObProxy not healthy"
                fi
            else
              echo "ObProxy not exist"
            fi
        else
            restart=1
        fi

        if [ $restart -eq 1 ]
        then
          echo "ObProxy will running ..."
          if [ x$is_first == xtrue ];
          then
            if [ x$ENABLE_ROOT_SERVER == xtrue ];
            then
              ($OBPROXY_ROOT/bin/obproxy -p $OBPROXY_PORT $OBPROXY_APP_NAME_ARG -c $ROOT_SERVER_CLUSTER_NAME -r "$ROOT_SERVER_LIST" -o obproxy_config_server_url=''"$OBPROXY_OPT_LOCAL")
            else
              ($OBPROXY_ROOT/bin/obproxy -p $OBPROXY_PORT $OBPROXY_APP_NAME_ARG -o obproxy_config_server_url=''$OBPROXY_CONFIG_SERVER_URL''"$OBPROXY_OPT_LOCAL")
            fi
          else
            if [ x$ENABLE_ROOT_SERVER == xtrue ];
            then
              ($OBPROXY_ROOT/bin/obproxy -p $OBPROXY_PORT $OBPROXY_APP_NAME_ARG -c $ROOT_SERVER_CLUSTER_NAME -r "$ROOT_SERVER_LIST" -o obproxy_config_server_url='')
            else
              ($OBPROXY_ROOT/bin/obproxy -p $OBPROXY_PORT $OBPROXY_APP_NAME_ARG -o obproxy_config_server_url=''$OBPROXY_CONFIG_SERVER_URL'')
            fi
          fi
        fi
        is_first=false;
        sleep 1
    done
}

function kill_obproxy()
{
    ps -C obproxy | grep obproxy |awk '{print $1}'|xargs kill -$1
}

function is_obproxy_exists()
{
    process_count=`ps -C obproxy | grep obproxy |wc -l`
    return $process_count
}

function is_obproxy_healthy()
{
  echo hi |nc 127.0.0.1 $OBPROXY_PORT -w 2 > /dev/null 2>&1
  return $?
}

function is_checkalive_exists()
{
    process_count=`ps -fC obproxyd.sh | grep obproxyd.sh |grep '\-c checkalive' | wc -l`
    return $process_count
}

function kill_all_checkalive()
{
    ps -fC obproxyd.sh | grep obproxyd.sh |grep '\-c checkalive' |awk '{print $2}'|xargs kill -9
}


while getopts c:n:i:ahp:t:rs:u: opt
do
    case $opt in
        c)
            OBPROXY_CMD=$OPTARG
            ;;
        n)
            OBPROXY_APP_NAME=$OPTARG
            ;;
        i)
            OBPROXY_IDC_NAME=$OPTARG
            ;;
        a)
            STATUS_ALL_OBPROXY='1'
            ;;
        p)
            OBPROXY_PORT=$OPTARG
            ;;
        t)
            ROOT_SERVER_LIST=$OPTARG
            ;;
        r)
            ENABLE_ROOT_SERVER='true'
            ;;
        s)
            ROOT_SERVER_CLUSTER_NAME=$OPTARG
            ;;
        u)
            OBPROXY_CONFIG_SERVER_URL=$OPTARG
            ;;
        h)
            help
            exit 0
            ;;
        *)
            fail_msg "Invalid parameter"
            help
            exit 255
            ;;
    esac
done

case $OBPROXY_CMD in
    start)
        check_dir
        check_opt
        start
        ;;
    stop)
        stop
        ;;
    status)
        status
        ;;
    checkalive)
        check_dir
        check_opt
        checkalive
        ;;
    *)
        fail_msg "COMMAND not support: $OBPROXY_CMD"
        help
        exit -1
        ;;
esac
