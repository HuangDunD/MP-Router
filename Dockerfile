# 使用 Ubuntu 24.04 作为基础镜像 (默认 GCC 13+，很好地支持 C++20)
FROM ubuntu:24.04

# 避免交互式前端的提示
ARG DEBIAN_FRONTEND=noninteractive

# 1. 安装基础编译工具和系统依赖
# 注意：移除了 libpq-dev，添加了编译 PG 所需的依赖 (readline, zlib, flex, bison)
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    wget \
    libssl-dev \
    pkg-config \
    libreadline-dev \
    zlib1g-dev \
    flex \
    bison \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

# 设置环境变量
ENV HOME=/root
WORKDIR /root

# 创建你的 CMake 寻找的目录结构
RUN mkdir -p /root/local/include && mkdir -p /root/local/lib

# -----------------------------------------------------------------------------
# 2. 安装 GKlib (METIS 的依赖)
# -----------------------------------------------------------------------------
RUN git clone https://github.com/KarypisLab/GKlib.git && \
    cd GKlib && \
    make config prefix=/root/local && \
    make && \
    make install && \
    cd .. && rm -rf GKlib

# -----------------------------------------------------------------------------
# 3. 安装 METIS
# -----------------------------------------------------------------------------
RUN git clone https://github.com/KarypisLab/METIS.git && \
    cd METIS && \
    make config prefix=/root/local && \
    make && \
    make install && \
    cd .. && rm -rf METIS

# -----------------------------------------------------------------------------
# 4. 源码编译安装 PostgreSQL 服务端 (包含 libpq 静态库)
# -----------------------------------------------------------------------------
# 这里选择 PostgreSQL 16.2 版本，你可以根据需要修改版本号
ENV PG_VERSION=16.2
RUN wget https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.gz && \
    tar -xzf postgresql-${PG_VERSION}.tar.gz && \
    cd postgresql-${PG_VERSION} && \
    # 配置安装路径为 /usr/local，这样 libpqxx 容易找到
    # 默认情况下，PostgreSQL 的 configure 会构建静态库 (libpq.a) 和动态库
    ./configure \
        --prefix=/usr/local \
        --with-openssl \
        --with-libxml \
        --without-icu && \
    # 编译并安装 (make world 包含文档和扩展，make install-world 安装所有内容)
    make -j$(nproc) world && \
    make install-world && \
    cd .. && rm -rf postgresql-${PG_VERSION} postgresql-${PG_VERSION}.tar.gz
# -----------------------------------------------------------------------------
# 4.5 初始化数据库并预执行 SQL 脚本
# -----------------------------------------------------------------------------

# 1. 创建 postgres 用户和组 (PostgreSQL 不能以 root 运行)
RUN groupadd -r postgres && useradd -r -g postgres postgres

# 2. 创建数据目录并授权
# 通常习惯放在 /usr/local/pgsql/data 或者 /var/lib/postgresql/data
ENV PGDATA=/usr/local/pgsql/data
RUN mkdir -p "$PGDATA" && chown -R postgres:postgres /usr/local/pgsql

# 3. 切换到 postgres 用户执行初始化、启动、SQL 设置、关闭
# 注意：这里必须在一个 RUN 指令内完成 "启动->执行->关闭"，否则构建层结束后进程会被杀掉
USER postgres

RUN /usr/local/bin/initdb -D "$PGDATA" -E UTF8 --locale=C && \
    /usr/local/bin/pg_ctl -D "$PGDATA" -l /tmp/logfile start -w && \
    /usr/local/bin/psql -c "CREATE USER system WITH PASSWORD '123456';" && \
    /usr/local/bin/psql -c "CREATE DATABASE smallbank OWNER system;" && \
    /usr/local/bin/psql -d smallbank -c "CREATE EXTENSION IF NOT EXISTS pageinspect;" && \
    /usr/local/bin/psql -c "ALTER USER system WITH SUPERUSER;" && \
    /usr/local/bin/pg_ctl -D "$PGDATA" stop

# 4. 恢复默认用户为 root (如果有后续操作需要 root)
# 或者保持为 postgres 以便容器启动时直接运行
USER root

# -----------------------------------------------------------------------------
# 5. 安装 libpqxx (PostgreSQL C++ 客户端)
# -----------------------------------------------------------------------------
# 由于上面安装了 PG 到 /usr/local，pg_config 应该在路径中
RUN git clone https://github.com/jtv/libpqxx.git && \
    cd libpqxx && \
    mkdir build && cd build && \
    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        -Dpqxx_TESTING=OFF \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DPostgreSQL_TYPE_INCLUDE_DIR=/usr/local/include \
        -DPostgreSQL_TYPE_LIBRARY=/usr/local/lib/libpq.a && \
    make -j$(nproc) && \
    make install && \
    cd ../.. && rm -rf libpqxx
