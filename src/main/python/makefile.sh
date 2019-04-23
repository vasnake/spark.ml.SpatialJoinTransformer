#!/usr/bin/env bash
# -*- mode: shell; coding: utf-8 -*-
# (c) Valik mailto:vasnake@gmail.com

# http://kvz.io/blog/2013/11/21/bash-best-practices/
# Use set -o pipefail in scripts to catch mysqldump fails in e.g. mysqldump |gzip.
# The exit status of the last command that threw a non-zero exit code is returned
set -o pipefail
# Use set -o errexit (a.k.a. set -e) to make your script exit when a command fails
# Then add || true to commands that you allow to fail
set -o errexit
# Use set -o nounset (a.k.a. set -u) to exit when your script tries to use undeclared variables
set -o nounset
# Use set -o xtrace (a.k.a set -x) to trace what gets executed. Useful for debugging
set -o xtrace

# Set magic variables for current file, basename, and directory at the top of your script for convenience
__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
__root="$(cd "$(dirname "${__dir}")" && pwd)"
__file="${__dir}/$(basename "${BASH_SOURCE[0]}")"

arg1="${1:-}"
arg2="${2:-}"
__args=($@)
__argsLen=${#@}

PYTHON_VERSION=3.5.5
PIP_VERSION=10.0.1
PYTEST_VERSION=3.4.2
PIPENV_VERSION=11.9.0
PYENV_ROOT=${HOME}/.pyenv
PYTHON_HOME=${PYENV_ROOT}/versions/${PYTHON_VERSION}
SPARK_HOME=${HOME}/.sparkenv/spark-2.4.1-bin-hadoop2.7
PROJECT_DIR=${__dir}/../../..

pushd ${__dir}

main() {
    echo -e "timestamp: $(date -u +"%Y-%m-%d %H:%M:%S UTC")"

    if [ ${__argsLen} -ge 1 ]; then
        if [ "${arg1}" = "setup-python" ]; then
            setupPython
        elif [ "${arg1}" = "setup-venv" ]; then
            setupVEnv
        elif [ "${arg1}" = "install-module" ]; then
            installModule
        elif [ "${arg1}" = "run-tests" ]; then
            runTests
        else
            errorExit "Unknown command '${arg1}'"
        fi
    else
        errorExit "You have to pass parameters. See ${__file} source code."
    fi
}

setupPython() {
# setup pyenv, python, pipenv:
    if ! command -v pyenv 1>/dev/null 2>&1; then
        echo "pyenv is not installed" >&2

        sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
            libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
            xz-utils tk-dev libffi-dev liblzma-dev python-openssl git

        curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash

        echo "export PATH=\"${PYENV_ROOT}/bin:\$PATH\"" >> ~/.bash_profile
        echo "eval \"\$(pyenv init -)\"" >> ~/.bash_profile
        echo "eval \"\$(pyenv virtualenv-init -)\"" >> ~/.bash_profile
        source ~/.bash_profile
    fi

    pyenv update

    pyenv install ${PYTHON_VERSION}

    pushd ${PYTHON_HOME}/bin
    ./pip install pip==${PIP_VERSION}
    ./pip install pipenv==${PIPENV_VERSION}
}

setupVEnv() {
# setup module env
    ${PYTHON_HOME}/bin/pipenv install --python ${PYTHON_HOME}/bin/python
# show env path
    ${PYTHON_HOME}/bin/pipenv --venv
    # "${HOME}/.local/share/virtualenvs/python-Db3iNNvE
}

installModule() {
# install module
    ${PYTHON_HOME}/bin/pipenv install -e . --dev --skip-lock
    ${PYTHON_HOME}/bin/pipenv install pytest==${PYTEST_VERSION} --dev --skip-lock
    #${PYTHON_HOME}/bin/pipenv install twine==1.10.0 --dev --skip-lock
    #${PYTHON_HOME}/bin/pipenv install wheel==0.30.0 --dev --skip-lock
}

runTests() {
# run tests
# https://docs.pytest.org/en/latest/usage.html
    ${PYTHON_HOME}/bin/pipenv --venv
    # ${PYTHON_HOME}/bin/pipenv run pytest -vv --junitxml=./test-report
    ${PYTHON_HOME}/bin/pipenv run pytest -vv -s -x
}

errorExit() {
    echo "$1" 1>&2
    exit 1
}

main

if [ "$?" = "0" ]; then
    echo "OK, command executed"
    exit 0
else
    errorExit "ERROR, can't do"
fi

popd

function ideaSetup() {
    read -r -d '' MSG << EOF
# python support in idea
File -> Project Structure -> (Modules | Facets) ->
Python ->
... ->
(+) ->
Python SDK ->
Add local ->
Existing environment ->
$(pipenv --venv)/bin/python # ~/.local/share/virtualenvs/python-Db3iNNvE/bin/python
OK ->
OK ->
select Python Interpreter ->
OK
EOF
    echo ${MSG}
}

function getSpark() {
    mkdir ~/.sparkenv && pushd $_
    wget http://mirror.linux-ia64.org/apache/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz
    tar -xzvf spark-2.4.1-bin-hadoop2.7.tgz && rm $_
    export SPARK_HOME=${HOME}/.sparkenv/spark-2.4.1-bin-hadoop2.7
    popd
    cat <<EOF >> .env
PYSPARK_TEST_JARS=${PROJECT_DIR}/target/scala-2.12/spark-transformer-spatialjoin-assembly-0.0.2-SNAPSHOT.jar
PYTHON_BASE_DIR=${__dir}
MODEL_TMP_DIR=/tmp/spatial-join-transformer/it
SPARK_HOME=${SPARK_HOME}
PYSPARK_SUBMIT_ARGS="--master local[2] --conf spark.jars=${PYSPARK_TEST_JARS} --conf spark.checkpoint.dir=/tmp/checkpoints pyspark-shell"
EOF
}
