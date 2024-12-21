set -x
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
# export MIMALLOC_VERBOSE=2

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

export FILE_SIZE_MB=$((1024*10))
export WORKER_NUM=30
export POOL_NUM=8
export IO_SERVER_NUM=${POOL_NUM}
export POOL_SIZE_MB=$((1024*1))
export IO_SIZE_Byte=$((512*8))
# export TEST_TYPE="Buffer_Pool+Pread" # Buffer_Pool+Pread or MMAP or PREAD
# 2 3 4 5 7 9 11 13 15 20 25 30
# for BLOCK_SIZE in 1 2 3 4 5 6 7 8
# do

export time=$(date "+%Y-%m-%d-%H:%M:%S")
export LOG_DIR=${CUR_DIR}/logs/${time}
mkdir -p ${LOG_DIR}
cp -r ./$0 ${LOG_DIR}/run.sh

echo 1 > /proc/sys/vm/drop_caches

./bin/graphscope_bufferpool ${FILE_SIZE_MB} ${WORKER_NUM} ${POOL_NUM} ${POOL_SIZE_MB} ${IO_SERVER_NUM} ${IO_SIZE_Byte} ${LOG_DIR} 
# cgexec -g memory:yz_15g

# done
# timeout 2m
# > ${LOG_DIR}/log.log
# ./bin/graphscope_bufferpool ${FILE_SIZE_GB} ${WORKER_NUM}
# cgexec -g memory:yz_574M
# ./bin/graphscope_bufferpool 
# cgexec -g memory:yz_256M ./bin/graphscope_bufferpool
# nohup ./bin/graphscope_bufferpool &

# sleep 10s
# timeout 100s perf record -F 999 -a -g -p `pidof graphscope_bufferpool` -o ${LOG_DIR}/perf.data
# kill `pidof graphscope_bufferpool`
