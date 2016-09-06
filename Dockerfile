#VERSION 1.0.0
FROM keboola/base-php56
MAINTAINER Miro Cillik <miro@keboola.com>

# Install dependencies
RUN yum -y --enablerepo=epel,remi,remi-php56 install php-devel
RUN yum -y --enablerepo=epel,remi,remi-php56 install php-odbc

## Cloudera Impala
RUN yum -y install unixODBC
RUN yum -y install cyrus-sasl-gssapi
RUN yum -y install cyrus-sasl-plain
ADD driver/ClouderaImpalaODBC-2.5.30.1011-1.el6.x86_64.rpm /tmp/ClouderaImpalaODBC-2.5.30.1011-1.el6.x86_64.rpm
RUN ln  -s  /usr/lib64/libsasl2.so.3  /usr/lib64/libsasl2.so.2
RUN rpm -ivh ClouderaImpalaODBC* --nodeps

RUN cp -Rf /opt/cloudera/impalaodbc/Setup/* /etc/
ADD driver/odbc.ini /etc/odbc.ini
ADD driver/cloudera.impalaodbc.ini /etc/cloudera.impalaodbc.ini
ADD driver/cloudera.impalaodbc.ini /opt/cloudera/impalaodbc/lib/64/cloudera.impalaodbc.ini
RUN ln -s /usr/lib64/libodbccr.so.2 /usr/lib64/libodbccr.so

ENV ODBCSYSINI /etc
ENV ODBCINI /etc/odbc.ini
ENV SIMBAINI /opt/cloudera/impalaodbc/lib/64/cloudera.impalaodbc.ini

# Run Application
ADD . /code
WORKDIR /code
RUN echo "memory_limit = -1" >> /etc/php.ini
RUN composer install --no-interaction

ENTRYPOINT php ./run.php --data=/data
