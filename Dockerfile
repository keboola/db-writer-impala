FROM centos:centos7

# Image setup
WORKDIR /tmp
RUN rpm -Uvh https://mirror.webtatic.com/yum/el7/epel-release.rpm && \
	rpm -Uvh http://rpms.famillecollet.com/enterprise/remi-release-7.rpm && \
	yum -y --enablerepo=epel,remi,remi-php73 upgrade && \
	yum -y --enablerepo=epel,remi,remi-php73 install \
		epel-release \
		git \
		unzip \
		php \
		php-cli \
		php-common \
		php-mbstring \
		php-pdo \
		php-xml \
		php-devel \
		php-odbc \
		&& \
	yum clean all && \
	echo "date.timezone=UTC" >> /etc/php.ini && \
	echo "memory_limit = -1" >> /etc/php.ini && \
	curl -sS https://getcomposer.org/installer | php && \
	mv composer.phar /usr/local/bin/composer

## Cloudera Impala
RUN yum -y install unixODBC cyrus-sasl-gssapi cyrus-sasl-plain
ADD driver/ClouderaImpalaODBC-2.6.7.1007-1.x86_64.rpm /tmp/ClouderaImpalaODBC-2.5.30.1011-1.el6.x86_64.rpm
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

CMD php ./run.php --data=/data
