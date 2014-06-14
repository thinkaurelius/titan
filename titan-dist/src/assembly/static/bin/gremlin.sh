#!/bin/bash

# Returns the absolute path of this script regardless of symlinks
abs_path() {
    # From: http://stackoverflow.com/a/246128
    #   - To resolve finding the directory after symlinks
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

case `uname` in
  CYGWIN*)
    CP=`abs_path`/../conf
    CP=$CP;$( echo `abs_path`/../lib/*.jar . | sed 's/ /;/g')
    CP=$CP;$(find -L `abs_path`/../ext/ -name "*.jar" | tr '\n' ';')
    ;;
  *)
    CP=`abs_path`/../conf
    CP=$CP:$( echo `abs_path`/../lib/*.jar . | sed 's/ /:/g')
    CP=$CP:$(find -L `abs_path`/../ext/ -name "*.jar" | tr '\n' ':')
esac
#echo $CP

# Find Java
if [ "$JAVA_HOME" = "" ] ; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ] ; then
    JAVA_OPTIONS="-Xms32m -Xmx512m"
fi

# Launch the application
if [ "$1" = "-e" ]; then
  k=$2
  if [ $# -gt 2 ]; then
    for (( i=3 ; i < $# + 1 ; i++ ))
    do
      eval a=\${${i}}
      k="$k \"$a\""
    done
  fi

  eval $JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.thinkaurelius.titan.tinkerpop.gremlin.ScriptExecutor $k
else
  if [ "$1" = "-v" ]; then
    $JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.tinkerpop.gremlin.Version
  else
    $JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.thinkaurelius.titan.tinkerpop.gremlin.Console $@
  fi
fi

# Return the program's exit code
exit $?
