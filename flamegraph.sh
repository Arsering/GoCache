set -x

export FLAME_GRAPH_DIR=/data/zhengyang/data/FlameGraph
export DIR=/data/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2024-05-11-08:46:05

cd ${DIR}
perf script -i perf.data > out.perf
${FLAME_GRAPH_DIR}/stackcollapse-perf.pl out.perf > out.folded
${FLAME_GRAPH_DIR}/flamegraph.pl out.folded > perf.svg