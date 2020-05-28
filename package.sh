APP=crdt-module
Version=1.0.1
Package=$APP-$Version

if [ -f $package ]; then
    echo remove $Package
    rm -rf $Package
fi
echo make clean
mkdir $Package
ls -a | egrep -v "Debug|^\.|$APP" | xargs -J %  cp -r %  $Package
echo create $Package.tar.gz...
tar -czf $Package.tar.gz $Package
rm -rf $Package
