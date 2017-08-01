# import base Cloudera Hadoop Single Node Docker image
FROM cloudera/quickstart:latest 

# install packages for MySQL 
RUN yum -y install unixODBC-devel
RUN yum -y install mysql-connector-odbc
RUN yum install -y zlib-dev openssl-devel sqlite-devel bzip2-devel

# install pyodbc from source, for some reason pip install pyodbc is not working
RUN yum -y install wget
RUN yum -y install python-devel
RUN wget https://pypi.python.org/packages/03/9a/7ed24041fbe08c4aa19a8da3a1f27dacdf0c355352dcbb053ab0fda4152f/pyodbc-4.0.2.tar.gz#md5=7bcb56dbc6a69fc414de21d18b3ca3d4
RUN tar -xvf pyodbc-4.0.2.tar.gz
WORKDIR pyodbc-4.0.2
RUN python setup.py build
RUN python setup.py install

# go back to base thrive folder and run script to copy example data to HDFS
WORKDIR /thrive/example
#ADD hdfs_data_copy.sh /thrive/example/docker/hdfs_data_copy.sh
#CMD ["/thrive/example/docker/hdfs_data_copy.sh"]

# export env variables
ENV OOZIE_URL http://quickstart.cloudera:11000/oozie
