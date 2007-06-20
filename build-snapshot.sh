VERSION=0.2.1
ALPHATAG=`git-show-ref --hash=10 refs/heads/master`
TARBALL=sugar-datastore-$VERSION-git$ALPHATAG.tar.bz2

rm sugar-datastore-$VERSION.tar.bz2

make distcheck

mv sugar-datastore-$VERSION.tar.bz2 $TARBALL
