set -x

cd build

rm -rf ./*
cmake ..
make -j

if [ $? -ne 0 ]; then
    echo -e "\033[41;36m Compile failed \033[0m"
    exit
else
    echo -e "\033[41;36m Compile succeed \033[0m"
fi

cd ..

CUR_DIR=.

export time=$(date "+%Y-%m-%d-%H:%M:%S")
export LOG_DIR=${CUR_DIR}/logs/${time}
mkdir -p ${LOG_DIR}
cp -r ./$0 ${LOG_DIR}/run.sh


echo 1 > /proc/sys/vm/drop_caches

export FILE_SIZE_GB=32
export WORKER_NUM=1
export POOL_NUM=64
export IO_SERVER_NUM=64
export POOL_SIZE_GB=1
export TEST_TYPE="Buffer_Pool+Pread" # Buffer_Pool+Pread or MMAP or PREAD
sudo ./bin/graphscope_bufferpool ${FILE_SIZE_GB} ${WORKER_NUM} ${POOL_NUM} ${POOL_SIZE_GB} ${IO_SERVER_NUM} ${TEST_TYPE} > ${LOG_DIR}/log.log

# > ${LOG_DIR}/log.log
# ./bin/graphscope_bufferpool ${FILE_SIZE_GB} ${WORKER_NUM}
# cgexec -g memory:yz_574M
# ./bin/graphscope_bufferpool 
# cgexec -g memory:yz_256M ./bin/graphscope_bufferpool
# nohup ./bin/graphscope_bufferpool &
# sleep 2s
# nohup perf record -F 999 -a -g -p `pidof graphscope_bufferpool` -o ./perf.data &
