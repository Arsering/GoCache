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

echo 1 > /proc/sys/vm/drop_caches

export FILE_SIZE_GB=16
export WORKER_NUM=10
export POOL_NUM=100
export POOL_SIZE_GB=4
./bin/graphscope_bufferpool ${FILE_SIZE_GB} ${WORKER_NUM} ${POOL_NUM} ${POOL_SIZE_GB} > ${LOG_DIR}/log.log 

# ./bin/graphscope_bufferpool ${FILE_SIZE_GB} ${WORKER_NUM}
# cgexec -g memory:yz_574M
# ./bin/graphscope_bufferpool 
# cgexec -g memory:yz_256M ./bin/graphscope_bufferpool
# nohup ./bin/graphscope_bufferpool &
# sleep 2s
# nohup perf record -F 999 -a -g -p `pidof graphscope_bufferpool` -o ./perf.data &
