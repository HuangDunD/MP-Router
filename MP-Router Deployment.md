# 部署GKlib 路径在~/local/
git clone --recursive https://github.com/Kleenelan/GKlib.git
cd GKlib/
make config cc=gcc openmp=set prefix=~/local
make
make install

# 安装Metis 路径在~/local/
git clone --recursive https://github.com/Kleenelan/METIS.git
cd METIS/
make config openmp=1 cc=gcc prefix=~/local
make
make install

# 源码编译安装postgres
## 安装依赖项
sudo apt-get install build-essential cmake zlib perl readline readline-devel zlib zlib-devel perl tcl openssl ncurses-devel openldap pam perl-IPC-Run libicu-deve
## 下载Postgres安装包
wget https://ftp.postgresql.org/pub/source/v12.1/postgresql-12.1.tar.gz
## 解压并编译安装
tar -xzf postgresql-12.1.tar.gz
cd postgresql-12.1/
./configure --prefix=~/local
make 
make install