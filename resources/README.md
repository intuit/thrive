### Steps for setting up Thrive in a new environment

#### Metadata setup
    cd thrive/metadata
    ./md_setup.sh -f <path/to/metadata/credentials>
    
#### Copy Oozie share lib to HDFS
    hadoop fs -put \
    /opt/cloudera/parcels/CDH/lib/oozie/libserver/oozie-sharelib-streaming-4.0.0-cdh5.1.5.jar \
    oozielib
 