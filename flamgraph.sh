export FLAME_GRAPH_DIR=/data_client2/zhengyang/data/FlameGraph
export DIR=/data_client2/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2024-04-26-23:02:49

cd ${DIR}
perf script -i perf.data > out.perf
${FLAME_GRAPH_DIR}/stackcollapse-perf.pl out.perf > out.folded
${FLAME_GRAPH_DIR}/flamegraph.pl out.folded > perf.svg