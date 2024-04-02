CUR_DIR=.
export iodepth=1
export numjobs=4

for numjobs in 100
do
    export time=$(date "+%Y-%m-%d-%H:%M:%S")
    export LOG_DIR=${CUR_DIR}/logs/${time}
    mkdir -p ${LOG_DIR}
    cp -r ./$0 ${LOG_DIR}/fio.sh

    fio --ioengine=psync --filename=./tests/db/test1.db --direct=1 --rw=randread --bs=4k --group_reporting --name=writeprepae  --size=80G --numjobs=${numjobs} --iodepth=${iodepth} --time_based --runtime=100 --output=${LOG_DIR}/fio.log
done