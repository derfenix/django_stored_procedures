#!/usr/bin/env bash

VERSION="$1"
if [[ "${VERSION}" == "" ]]
then
    echo "Usage: $0 <new_version>"
    exit 10
fi

sed -r -i -e "s/version='.+'/version='${VERSION}'/g" setup.py
python ./setup.py dsist
gpg2 --detach-sign -a dist/django_stored_procedures-"${VERSION}".tar.gz
twine upload dist/django_stored_procedures-"$VERSION".tar.gz dist/django_stored_procedures-"$VERSION".tar.gz.asc
git commit -a -m "Bump version"
git tag v${VERSION}
git push --tags
