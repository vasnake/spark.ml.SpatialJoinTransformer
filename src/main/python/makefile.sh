#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

PYTHON_VERSION=3.7.3
PYTHON_HOME=${HOME}/.pyenv/versions/${PYTHON_VERSION}

function getspark() {
    mkdir ~/.sparkenv
    pushd ~/.sparkenv
    wget http://mirror.linux-ia64.org/apache/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz
    tar -xzvf spark-2.4.1-bin-hadoop2.7.tgz
    export SPARK_HOME=${HOME}/.sparkenv/spark-2.4.1-bin-hadoop2.7
    popd
}

# setup python+pipenv:

#pyenv install ${PYTHON_VERSION}
#pushd ${PYTHON_HOME}/bin
#./pip install pip==10.0.1
#./pip install pipenv==2018.05.18

# setup module env

#${PYTHON_HOME}/bin/pipenv install --python ${PYTHON_HOME}/bin/python
## show env path
${PYTHON_HOME}/bin/pipenv --venv # "${HOME}/.local/share/virtualenvs/python-Fr9IlheY"

# install module

#${PYTHON_HOME}/bin/pipenv install -e . --dev --skip-lock
#${PYTHON_HOME}/bin/pipenv install pytest==3.4.2 --dev --skip-lock
#${PYTHON_HOME}/bin/pipenv install twine==1.10.0 --dev --skip-lock
#${PYTHON_HOME}/bin/pipenv install wheel==0.30.0 --dev --skip-lock

# run tests
# https://docs.pytest.org/en/latest/usage.html
# ${PYTHON_HOME}/bin/pipenv run pytest -vv --junitxml=./test-report
# ${PYTHON_HOME}/bin/pipenv run pytest -vv -s -x
