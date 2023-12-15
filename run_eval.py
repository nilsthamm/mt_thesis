import subprocess
import sys
import requests
#
PROJECT_DIR = '/hpi/fs00/home/nils.thamm/clion/pear-tree/'
CMAKE_DIR = PROJECT_DIR+'cmake-build-release-remote-host-nvram04/'

SINGLE_THREAD = '18'
SINGLE_SOCKET = '18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35'
SINGLE_SOCKET_HYPERTHREADING = '18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71'

"""
# /usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_existing_systems -- -j9
# /usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_scalability -- -j9
# /usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_scalability_hyperthreading -- -j9
# /usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_fpr_hyperthreading -- -j9
# /usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_persistence_degree -- -j9
# /usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_recovery -- -j9
/usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_persisting -- -j9
/usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_node_size -- -j9
# /usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_prefetching -- -j9
# /usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_pmem_unaligned -- -j9
# /usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_cpu_cache_aligned -- -j9
# /usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_fpr_mixed_workload -- -j9
# /usr/bin/cmake --build /hpi/fs00/home/nils.thamm/clion/pear-tree/cmake-build-release-remote-host-nvram04 --target eval_mixed_workload -- -j9
python3 clion/pear-tree/notification.py
"""
# PROJECT_DIR = '/tmp/tmp.j3ecp41Ec8/'
# CMAKE_DIR = PROJECT_DIR+'cmake-build-release/'


RUN_FESTIVAL=False
RUN_EXISTING_SYSTEMS_PEAR=False
RUN_EXISTING_SYSTEMS_FPR=False
RUN_SCALABILITY_PEAR=False
RUN_SCALABILITY_FPR=False
RUN_PERSISTENCE_DEGREE=True
RUN_RECOVERY=False
RUN_PERSISTING=False
RUN_NODE_PERFORMANCE_BASELINE=False
RUN_LOCKING=False
RUN_SIMD=False
RUN_NODE_SIZES=False
RUN_PREFETCHING=False
RUN_MIXED_WORKLOAD_PEAR=False
RUN_MIXED_WORKLOAD_FPR=False
RUN_UNALIGNED_NODE=False
RUN_CPU_CACHE_ALIGNE_NODE=False

def telegram_bot_sendtext(bot_message):

    bot_token = '1641180059:AAGcGpaUTI0jQTAOb7_Mk7R-93fo7fDyxCM'
    bot_chatID = '9125767'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()




def execute_mixed_workload_benchmark(name, path_post_fix, executable_name):
    print("Starting " + name + " benchmarks")
    arguments = ['numactl','--membind','1','--physcpubind',SINGLE_SOCKET,CMAKE_DIR+'src/benchmarks/'+executable_name,'/hpi/fs00/home/nils.thamm/clion/pear-tree/'+file_out_name+path_post_fix+'.csv']
    process = subprocess.Popen(arguments, stdout=std_out_file, stderr=std_err_file)
    process.wait()
    std_out_file.flush()
    std_err_file.flush()
    if(process.returncode):
        print(telegram_bot_sendtext("*[FAILED]* " + name + " failed with return code " + str(process.returncode)))
    else:
        print(telegram_bot_sendtext("*[SUCCESS]* " + name + " finished"))

def execute_google_benchmark(name, core_id_list, path_post_fix, executable_name, filter_regex=None):
    print("Starting " + name + " benchmarks")
    if filter_regex:
        arguments = ['numactl','--membind','1','--physcpubind',core_id_list,CMAKE_DIR+'src/benchmarks/'+executable_name,'--benchmark_out_format=csv','--benchmark_out=/hpi/fs00/home/nils.thamm/clion/pear-tree/'+file_out_name+path_post_fix+'.csv','--benchmark_filter='+filter_regex]
    else:
        arguments = ['numactl','--membind','1','--physcpubind',core_id_list,CMAKE_DIR+'src/benchmarks/'+executable_name,'--benchmark_out_format=csv','--benchmark_out=/hpi/fs00/home/nils.thamm/clion/pear-tree/'+file_out_name+path_post_fix+'.csv']
    process = subprocess.Popen(arguments, stdout=std_out_file, stderr=std_err_file)
    process.wait()
    std_out_file.flush()
    std_err_file.flush()
    if(process.returncode):
        print(telegram_bot_sendtext("*[FAILED]* " + name + " failed with return code " + str(process.returncode)))
    else:
        print(telegram_bot_sendtext("*[SUCCESS]* " + name + " finished"))

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage python3 execute_benchmark.py out_file_name std_out_file std_err_file")
        exit()
    file_out_name = sys.argv[1]
    std_out_file = open(sys.argv[2], "wb", buffering=0)
    std_err_file = open(sys.argv[3], "wb", buffering=0)

    if RUN_EXISTING_SYSTEMS_PEAR:
        execute_google_benchmark("Existing Systems PEAR comparison", SINGLE_THREAD, 'existing_systems_pear', 'eval_existing_systems')

    if RUN_EXISTING_SYSTEMS_FPR:
        execute_google_benchmark("Existing Systems FPR", SINGLE_THREAD, 'existing_systems_fpr', 'eval_existing_systems_fpr')

    if RUN_SCALABILITY_PEAR:
        execute_google_benchmark("Scalability no hyperthreading PEAR", SINGLE_SOCKET, 'scalability_pear_single_socket', 'eval_scalability')
        execute_google_benchmark("Scalability hyperthreading PEAR", SINGLE_SOCKET_HYPERTHREADING, 'scalability_pear_hyper', 'eval_scalability_hyperthreading')

    if RUN_SCALABILITY_FPR:
        execute_google_benchmark("Scalability no hyperthreading FPR", SINGLE_SOCKET, 'scalability_fpr_single_socket', 'eval_fpr')
        execute_google_benchmark("Scalability hyperthreading FPR", SINGLE_SOCKET_HYPERTHREADING, 'scalability_fpr_hyper', 'eval_fpr_hyperthreading')

    if RUN_PERSISTENCE_DEGREE:
        execute_google_benchmark("Persistence Degree", SINGLE_SOCKET, 'persistence_degree', 'eval_persistence_degree')

    if RUN_RECOVERY:
        execute_google_benchmark("Recovery", SINGLE_SOCKET, 'recovery', 'eval_recovery')

    if RUN_PERSISTING:
        execute_google_benchmark("Persisting", SINGLE_SOCKET, 'persisting', 'eval_persisting')

    if RUN_NODE_PERFORMANCE_BASELINE:
        execute_google_benchmark("Node performance baseline", SINGLE_SOCKET, 'node_performance_baseline', 'eval_node_performance_baseline')

    if RUN_LOCKING:
        execute_google_benchmark("Locking", SINGLE_SOCKET, 'locking', 'eval_locking')

    if RUN_SIMD:
        execute_google_benchmark("SIMD", SINGLE_SOCKET, 'simd', 'eval_simd')

    if RUN_NODE_SIZES:
        execute_google_benchmark("Node sizes", SINGLE_SOCKET, 'node_size', 'eval_node_size')

    if RUN_PREFETCHING:
        execute_google_benchmark("Prefetching", SINGLE_SOCKET, 'prefetching', 'eval_prefetching')

    if RUN_UNALIGNED_NODE:
        execute_google_benchmark("Unaligned Nodes", SINGLE_SOCKET, 'pmem_unaligned', 'eval_pmem_unaligned')

    if RUN_CPU_CACHE_ALIGNE_NODE:
        execute_google_benchmark("CPU Cache aligned Node", SINGLE_SOCKET, 'cpu_cache_aligned', 'eval_cpu_cache_aligned')

    if RUN_MIXED_WORKLOAD_PEAR:
        execute_mixed_workload_benchmark("Mixed Workload PEAR", 'mixed_workload_pear', 'eval_mixed_workload')

    if RUN_MIXED_WORKLOAD_FPR:
        execute_mixed_workload_benchmark("Mixed Workload FPR", 'mixed_workload_fpr', 'eval_fpr_mixed_workload')



