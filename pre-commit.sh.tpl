#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

function check {
    name=$1
    prefix=$2
    files=$(git diff --cached --name-only --diff-filter=ACM | grep $prefix)
    shift; shift;
    echo -n "checking $name ";
    if [ -z "$files" ]; then 
        echo -e "${CYAN}[NOT APPLICABLE]${NC}"
        exit 0;
    fi

    cd $(git rev-parse --show-toplevel)
    
    out=$($@ $files)
    if [ $? -eq 0 ];
    then
        echo -e "${GREEN}[OK]${NC}"
        cd -
    else
        echo -e "${RED}[FAILED]${NC}"
        echo -e "Please inspect the output below and run make fmt to fix automatically\n"
        echo -e "$out"
        exit 1
    fi
}

check rustfmt .rs rustfmt --check --edition=2018
