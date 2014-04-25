#!/bin/bash

# build binary distributions for linux/amd64 and darwin/amd64

# export GOPATH=$(godep path):$GOPATH

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
rm -rfv $DIR/dist
mkdir -p $DIR/dist

for os in linux ; do
    echo "... building v$version for $os/$arch"
    BUILD=$(mktemp -d -t push)
    TARGET="push"
    GOOS=$os GOARCH=$arch CGO_ENABLED=0 make || exit 1
    make DESTDIR=$BUILD/$TARGET PREFIX= install
	
	mkdir -pv $BUILD/$TARGET/scripts
	cp -rv $DIR/scripts/* $BUILD/$TARGET/scripts/

    pushd $BUILD

    tar czvf $TARGET.tar.gz $TARGET
    mv $TARGET.tar.gz $DIR/dist
    popd
    make clean
done
