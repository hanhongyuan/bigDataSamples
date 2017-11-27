#!/usr/bin/env bash
shopt -s nullglob

ks_home=$(cd "`dirname $0`"/..; pwd)

echo "inspect java_home.........."
if [ -z "$JAVA_HOME" ]; then
   echo "Your JAVA_HOME is not configured, please configure and then execute again!"
   exit 1
else
   JAVA="$JAVA_HOME/bin/java"
fi

#recursion
function recursion(){
    for file in `ls $1`
    do
       if [ -d $1"/"$file ]; then
            recursion $1"/"$file
        else
            CLASSPATH=${CLASSPATH}:$1"/"$file
        fi
    done
}

recursion "$ks_home"/libs


$JAVA -cp $CLASSPATH -DksHome="$ks_home" com.unimas.kstream.Main "$@"
