FROM centos:7 as builder

ENV rocksdb_version 6.27.3
ENV golang_version 1.16.13

RUN mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.backup \
	&& curl https://mirrors.163.com/.help/CentOS7-Base-163.repo -o /etc/yum.repos.d/CentOS7-Base-163.repo \
	&& yum clean all && yum makecache \
	&& yum install -y epel-release \
	&& yum install -y --nogpgcheck\
		wget \
		libzstd libzstd-devel \
		snappy snappy-devel \
		zlib zlib-devel \
		bzip2 bzip2-devel \
		lz4-devel \
		make \
		gcc \
		which \
		gcc-c++ \
		python3 \
		perl \
		git \
	&& git clone --branch v${rocksdb_version} https://github.com/facebook/rocksdb.git \
	&& wget https://go.dev/dl/go${golang_version}.linux-amd64.tar.gz -q \
	&& rm -rf /usr/local/go \
	&& tar -C /usr/local -xzf go${golang_version}.linux-amd64.tar.gz 

RUN cd rocksdb \
	&& make static_lib -j 4

ADD . /app

RUN cd /app \
	&& CGO_CFLAGS="-I/rocksdb/include" \
		CGO_LDFLAGS="-L/rocksdb -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lbz2"  \
		/usr/local/go/bin/go build

FROM centos:7 

RUN mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.backup \
	&& curl https://mirrors.163.com/.help/CentOS7-Base-163.repo -o /etc/yum.repos.d/CentOS7-Base-163.repo \
	&& yum clean all && yum makecache \
	&& yum install -y epel-release \
	&& yum install -y --nogpgcheck \ 
		libzstd libzstd-devel \
		snappy snappy-devel \
		zlib zlib-devel \
		bzip2 bzip2-devel \
		lz4-devel 

COPY --from=builder /app/nebula-dump /app/nebula-dump

WORKDIR  /app