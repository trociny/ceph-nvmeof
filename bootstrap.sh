#!/bin/bash

CEPH_BUILD=/home/mgolub/ceph/ceph.ci/build

function abort_missing_dep {
    local executable="$1"
    local provided_by="$2"
    if type $executable > /dev/null 2>&1 ; then
        true
    else
        >&2 echo "ERROR: $executable not available"
        >&2 echo "Please install the $provided_by package for your OS, and try again."
        exit 1
    fi
}

if [ -z "$1" ]; then
    >&2 echo "usage: $0 </path/to/ceph/build>"
    exit 1
fi

CEPH_BUILD="$1"

if [ -d ./ceph_nvmeof_gateway ]; then
    true
else
    >&2 echo "The working directory, $(pwd), does not look like a ceph-nvmeof git clone."
    >&2 echo "Bailing out!"
    exit 1
fi

abort_missing_dep python3 python3-base
abort_missing_dep virtualenv python3-virtualenv

if [ -d ./venv ]; then
    >&2 echo "Detected an existing virtual environment - blowing it away!"
    rm -rf ./venv
fi

sed -i.orig -E "s/('(rados|rbd)',)/#\1/" setup.py
trap 'mv setup.py.orig setup.py' INT TERM EXIT

virtualenv --python=python3 --verbose venv

echo "export LD_LIBRARY_PATH=${CEPH_BUILD}/lib" >> venv/bin/activate
echo "export PYTHONPATH=${CEPH_BUILD}/lib/cython_modules/lib.3" >> venv/bin/activate

rm -f ceph.conf
ln -s ${CEPH_BUILD}/ceph.conf

source venv/bin/activate
pip install --editable .

>&2 echo
>&2 echo "ceph-nvmeof installation complete."
>&2 echo "Remember to do \"source venv/bin/activate\" before trying to run sesdev!"
>&2 echo "When finished, issue the \"deactivate\" command to leave the Python virtual environment."
