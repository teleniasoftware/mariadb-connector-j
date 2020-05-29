#!/bin/bash

set -x
set -e

###################################################################################################################
# test different type of configuration
###################################################################################################################

export DB_HOST=mariadb.example.com
export DB_PORT=3305
export DB_DATABASE=testj
export DB_USER=bob
export DB_OTHER=
export INNODB_LOG_FILE_SIZE=$(echo ${PACKET}| cut -d'M' -f 1)0M

cmd=( mvn clean test $ADDITIONNAL_VARIABLES -DjobId=${TRAVIS_JOB_ID}  )

###################################################################################################################
# launch docker server
###################################################################################################################
mysql=( mysql --protocol=tcp -ubob -h127.0.0.1 --port=3305 )
export COMPOSE_FILE=.travis/docker-compose.yml
docker-compose -f ${COMPOSE_FILE} up -d

###################################################################################################################
# wait for docker initialisation
###################################################################################################################

for i in {60..0}; do
    if echo 'SELECT 1' | "${mysql[@]}" &> /dev/null; then
        break
    fi
    echo 'data server still not active'
    sleep 1
done


if [ "$i" = 0 ]; then
    if [ -n "COMPOSE_FILE" ] ; then
        docker-compose -f ${COMPOSE_FILE} logs
    fi

    echo 'SELECT 1' | "${mysql[@]}"
    echo >&2 'data server init process failed.'
    exit 1
fi


###################################################################################################################
# run test suite
###################################################################################################################
echo "Running tests for JDK version: $TRAVIS_JDK_VERSION"

echo ${cmd}

"${cmd[@]}"