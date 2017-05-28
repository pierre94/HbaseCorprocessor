#!/usr/bin/env bash
BASE_HOME="`dirname $0`/.."

JAVA_HOME=~/java
HBASE_HOME=~/hbase
HADOOP_HOME=~/hadoop
JAVA_HEAP_MAX=-Xmx2000m

function print_usage(){
  echo "Usage: HQueuetool COMMAND"
  echo "       where COMMAND is one of:"
  echo "  -create             create Hqueue"
  echo "  -put                put message"
  echo "  -scan               scan hqueue"
}


if [ $# = 0 ]; then
  print_usage
  exit
fi

COMMAND=$1
case $COMMAND in
  # usage flags
  --help|-help|-h)
    print_usage
    exit
    ;;

  #core commands
  *)
    CLASS="tools.HQueueTools"
    CLASSPATH="."
    for jar in $BASE_HOME/target/lib/*.jar
    do
        CLASSPATH="$CLASSPATH:$jar"
    done
    hqueue_jar=`ls $BASE_HOME/target/userCopro*.jar`
    if [ $? -ne 0 ]; then
        exit 1
    fi
    CLASSPATH="$CLASSPATH:$hqueue_jar"
    export CLASSPATH="$CLASSPATH"

    exec $JAVA_HOME/bin/java $JAVA_HEAP_MAX -cp $CLASSPATH:`$HADOOP_HOME/bin/hadoop classpath`:`$HBASE_HOME/bin/hbase classpath` $CLASS "$@"
    ;;

esac
