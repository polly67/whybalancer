#!/usr/bin/env bash
set -euo pipefail

echo "=========================================================="
echo " Starting Linux Machine Diagnostic & Benchmark Suite"
echo "=========================================================="

# 1. System Info
echo "--- SYSTEM INFORMATION ---"
uname -a
lsb_release -a || cat /etc/os-release

echo ""
echo "--- CPU SPECS ---"
lscpu

echo ""
echo "--- RAM SPECS ---"
free -h
cat /proc/meminfo | grep -E "MemTotal|MemFree|MemAvailable|SwapTotal|SwapFree"

echo ""
echo "--- DISK SPECS ---"
lsblk
df -h /

# Ensure tools are installed
echo ""
echo "--- INSTALLING BENCHMARK TOOLS ---"
sudo apt-get update -y
sudo apt-get install -y sysbench fio jq

echo ""
echo "=========================================================="
echo " Running CPU Benchmarks"
echo "=========================================================="
NUM_CORES=$(nproc)
echo "Detected CPU cores: $NUM_CORES"

echo "Running Sysbench CPU (Single Thread)..."
sysbench cpu --cpu-max-prime=20000 --threads=1 run > sysbench_cpu_single.log
cat sysbench_cpu_single.log

echo "Running Sysbench CPU (Multi Thread: $NUM_CORES cores)..."
sysbench cpu --cpu-max-prime=20000 --threads="$NUM_CORES" run > sysbench_cpu_multi.log
cat sysbench_cpu_multi.log

echo ""
echo "=========================================================="
echo " Running Memory (RAM) Benchmarks"
echo "=========================================================="
echo "Running Sysbench Memory Read (10 GB)..."
sysbench memory --memory-block-size=1M --memory-total-size=10G --memory-oper=read run > sysbench_mem_read.log
cat sysbench_mem_read.log

echo "Running Sysbench Memory Write (10 GB)..."
sysbench memory --memory-block-size=1M --memory-total-size=10G --memory-oper=write run > sysbench_mem_write.log
cat sysbench_mem_write.log

echo ""
echo "=========================================================="
echo " Running Disk IOPS & Throughput Benchmarks (fio)"
echo "=========================================================="

TEST_DIR="/tmp/fio_test"
mkdir -p "$TEST_DIR"
cd "$TEST_DIR"

echo "Running 4K Random Read IOPS (fio)..."
fio --name=randread --filename=fio_randread.bin --ioengine=libaio --iodepth=32 --rw=randread --bs=4k --direct=1 --size=512M --numjobs=1 --runtime=15 --time_based --group_reporting --output-format=json > fio_randread.json
fio --name=randread --filename=fio_randread.bin --ioengine=libaio --iodepth=32 --rw=randread --bs=4k --direct=1 --size=512M --numjobs=1 --runtime=15 --time_based --group_reporting

echo "Running 4K Random Write IOPS (fio)..."
fio --name=randwrite --filename=fio_randwrite.bin --ioengine=libaio --iodepth=32 --rw=randwrite --bs=4k --direct=1 --size=512M --numjobs=1 --runtime=15 --time_based --group_reporting --output-format=json > fio_randwrite.json
fio --name=randwrite --filename=fio_randwrite.bin --ioengine=libaio --iodepth=32 --rw=randwrite --bs=4k --direct=1 --size=512M --numjobs=1 --runtime=15 --time_based --group_reporting

echo "Running 1M Sequential Read Throughput (fio)..."
fio --name=seqread --filename=fio_seqread.bin --ioengine=libaio --iodepth=16 --rw=read --bs=1M --direct=1 --size=512M --numjobs=1 --runtime=10 --time_based --group_reporting --output-format=json > fio_seqread.json
fio --name=seqread --filename=fio_seqread.bin --ioengine=libaio --iodepth=16 --rw=read --bs=1M --direct=1 --size=512M --numjobs=1 --runtime=10 --time_based --group_reporting

echo "Running 1M Sequential Write Throughput (fio)..."
fio --name=seqwrite --filename=fio_seqwrite.bin --ioengine=libaio --iodepth=16 --rw=write --bs=1M --direct=1 --size=512M --numjobs=1 --runtime=10 --time_based --group_reporting --output-format=json > fio_seqwrite.json
fio --name=seqwrite --filename=fio_seqwrite.bin --ioengine=libaio --iodepth=16 --rw=write --bs=1M --direct=1 --size=512M --numjobs=1 --runtime=10 --time_based --group_reporting

cd -

# Parse Metrics
echo ""
echo "=========================================================="
echo " Summary Parsing"
echo "=========================================================="

CPU_MODEL=$(lscpu | grep "Model name:" | sed 's/Model name:[ \t]*//' | head -n 1)
CPU_CORES=$(nproc)
CPU_SINGLE_EPS=$(grep "events per second:" sysbench_cpu_single.log | awk '{print $4}')
CPU_MULTI_EPS=$(grep "events per second:" sysbench_cpu_multi.log | awk '{print $4}')

MEM_TOTAL=$(free -h | awk '/^Mem:/ {print $2}')
MEM_AVAIL=$(free -h | awk '/^Mem:/ {print $7}')
MEM_READ_SPEED=$(grep "transferred" sysbench_mem_read.log | sed -E 's/.*[(]([0-9.]+ [A-Za-z/]+)[)].*/\1/')
MEM_WRITE_SPEED=$(grep "transferred" sysbench_mem_write.log | sed -E 's/.*[(]([0-9.]+ [A-Za-z/]+)[)].*/\1/')

READ_IOPS=$(jq -r '.jobs[0].read.iops' "$TEST_DIR/fio_randread.json" | cut -d'.' -f1)
WRITE_IOPS=$(jq -r '.jobs[0].write.iops' "$TEST_DIR/fio_randwrite.json" | cut -d'.' -f1)
READ_BW=$(jq -r '.jobs[0].read.bw_bytes' "$TEST_DIR/fio_randread.json" | awk '{printf "%.2f MB/s", $1/1024/1024}')
WRITE_BW=$(jq -r '.jobs[0].write.bw_bytes' "$TEST_DIR/fio_randwrite.json" | awk '{printf "%.2f MB/s", $1/1024/1024}')

SEQ_READ_BW=$(jq -r '.jobs[0].read.bw_bytes' "$TEST_DIR/fio_seqread.json" | awk '{printf "%.2f MB/s", $1/1024/1024}')
SEQ_WRITE_BW=$(jq -r '.jobs[0].write.bw_bytes' "$TEST_DIR/fio_seqwrite.json" | awk '{printf "%.2f MB/s", $1/1024/1024}')

# Cleanup fio files
rm -rf "$TEST_DIR"

# Print Final Summary
cat << EOF > benchmark_summary.md
# 🚀 Linux Machine Benchmark Results

## 1. 💻 CPU Specs & Performance
- **CPU Model:** $CPU_MODEL
- **Cores / vCPUs:** $CPU_CORES
- **Single-Core Speed:** $CPU_SINGLE_EPS events/sec (sysbench prime=20000)
- **Multi-Core Speed:** $CPU_MULTI_EPS events/sec (sysbench prime=20000)

## 2. 🧠 RAM (Memory) Capacity & Speed
- **Total RAM:** $MEM_TOTAL
- **Available RAM:** $MEM_AVAIL
- **Memory Read Throughput:** $MEM_READ_SPEED
- **Memory Write Throughput:** $MEM_WRITE_SPEED

## 3. 💽 Disk IOPS & Performance
- **Random 4K Read IOPS:** $READ_IOPS IOPS ($READ_BW)
- **Random 4K Write IOPS:** $WRITE_IOPS IOPS ($WRITE_BW)
- **Sequential 1M Read:** $SEQ_READ_BW
- **Sequential 1M Write:** $SEQ_WRITE_BW
EOF

cat benchmark_summary.md

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  cat benchmark_summary.md >> "$GITHUB_STEP_SUMMARY"
fi

echo "Benchmark completed successfully."
