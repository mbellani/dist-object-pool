
A [zookeepr recipe](http://zookeeper.apache.org/doc/trunk/recipes.html) for distributed object pool. There are  other implementations of object pool like [apache commons pool](http://commons.apache.org/proper/commons-pool/), while it solves the problem of having a local object pool there are times when you need an implemenation that can share resources accross processes. 

Before running tests, install the zookeeper lock recipe in the local repo

For local:
mvn install:install-file -Dfile=<PROJECT_ROOT>/thirdparty/zookeeper-3.4.5-recipes-lock.jar -DgroupId=org.apache.zookeeper.recipes \
    -DartifactId=lock -Dversion=3.4.5 -Dpackaging=jar

To Run the tests : mvn clean test.
