#!/bin/bash

BOLD=`tput bold`
NORMAL=`tput sgr0`
YELLOW='\033[1;33m'
ENDC='\033[0m'

function build_image() {
    dockerfile=$1
    remote_registry=$2
    tagged=$2
    buildcmd="docker build -t ${tagged} -f ${dockerfile} ."
    echo -e "${BOLD}$buildcmd${NORMAL}"
    $buildcmd
    pushcmd="docker push ${remote_registry}"
    echo -e "${BOLD}$pushcmd${NORMAL}"
    $pushcmd
    echo -e "${YELLOW}${BOLD}Image: ${remote_registry}${NORMAL}${ENDC}"
}

case "$1" in

base)
    version=""
    [[ ! -z $2 ]] && version=":${2}"
    build_image "Dockerfile" "registry.zilliz.com/milvus/celery${version}" "registry.zilliz.com/milvus/celery"
    ;;

apps)
    version=""
    [[ ! -z $2 ]] && version=":${2}"
    build_image "Dockerfile_apps" "registry.zilliz.com/milvus/celery-apps${version}" "registry.zilliz.com/milvus/celery-apps"
    ;;

*)
    echo "Usage: [option...] {base | apps}"
    echo "base,      Usage: build.sh base [tagname|] => registry.zilliz.com/milvus/celery:\${tagname}"
    echo "apps,      Usage: build.sh apps [tagname|] => registry.zilliz.com/milvus/celery-apps:\${tagname}"
    ;;
esac
