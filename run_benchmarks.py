import subprocess
import sys
import requests
#
PROJECT_DIR = '/hpi/fs00/home/nils.thamm/clion/pear-tree/'
CMAKE_DIR = PROJECT_DIR+'cmake-build-release-remote-host-nvram02/'

SINGLE_SOCKET = '3,4,7,8,11,12,13,16,17'
TWO_SOCKET = '0,1,2,5,6,9,10,14,15,3,4,7,8,11,12,13,16,17'
TWO_SOCKET_HYPERTHREADING = '0,1,2,5,6,9,10,14,15,36,37,38,41,42,45,46,50,51,3,4,7,8,11,12,13,16,17,39,40,43,44,47,48,49,52,53'

# PROJECT_DIR = '/tmp/tmp.j3ecp41Ec8/'
# CMAKE_DIR = PROJECT_DIR+'cmake-build-release/'

def telegram_bot_sendtext(bot_message):

    bot_token = ''
    bot_chatID = ''
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()

def execute_benchmark(std_out_file, name, memory_node_ids, core_id_list, path_post_fix, filter_regex):
    print("Starting " + name + " benchmarks")
    arguments = ['numactl','-i',memory_node_ids,'--physcpubind',core_id_list,CMAKE_DIR+'src/benchmarks/micro_benchmarks','--benchmark_out_format=csv','--benchmark_out=/hpi/fs00/home/nils.thamm/clion/pear-tree/'+file_out_name+path_post_fix+'.csv','--benchmark_filter='+filter_regex]
    process = subprocess.Popen(arguments, stdout=subprocess.PIPE)
    for c in iter(lambda: process.stdout.read(1) if process.stdout else b'', b''):
        std_out_file.write(c)
        sys.stdout.buffer.write(c)
    if(process.returncode):
        print(telegram_bot_sendtext("*[FAILED]* " + name + " failed with return code " + str(process.returncode)))
    else:
        print(telegram_bot_sendtext("*[SUCCESS]* " + name + " finished"))

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage python3 execute_benchmark.py out_file_name std_out_file [filter]")
        exit()
    file_out_name = sys.argv[1]
    std_out_file = open(sys.argv[2], "wb", buffering=0)
    if len(sys.argv) == 4:
        arguments = ['numactl','-i','1','--physcpubind','3,4,7,8,11,12,13,16,17',CMAKE_DIR + 'src/benchmarks/micro_benchmarks','--benchmark_out_format=csv','--benchmark_out='+PROJECT_DIR+file_out_name+'.csv','--benchmark_filter='+sys.argv[3]]
        # arguments = ['numactl','-i','0,1','--physcpubind','0,1,2,5,6,9,10,14,15,3,4,7,8,11,12,13,16,17',CMAKE_DIR+'src/benchmarks/micro_benchmarks','--benchmark_out_format=csv','--benchmark_out=/hpi/fs00/home/nils.thamm/clion/pear-tree/'+file_out_name,'--benchmark_filter='+sys.argv[2]]

        process = subprocess.Popen(arguments, stdout=subprocess.PIPE)
        for c in iter(lambda: process.stdout.read(1) if process.stdout else b'', b''):
            std_out_file.write(c)
            sys.stdout.buffer.write(c)
        if(process.returncode):
            telegram_bot_sendtext("*[FAILED]* Benchmarks failed with return code " + str(process.returncode))
        else:
            telegram_bot_sendtext("*[SUCCESS]* Benchmarks finished with return code 0")
    else:
        # execute_benchmark(std_out_file, "Recovery complete level", '1', SINGLE_SOCKET, 'recovery_c_level', 'BM_PEAR_RECOVERY_SZ_TAXI_COMPLETE_LEVEL_M.*OPTIMAL')
        execute_benchmark(std_out_file, "Recovery complete level XL", '1', SINGLE_SOCKET, 'recovery_c_level_xl', 'BM_PEAR_RECOVERY_SZ_TAXI_COMPLETE_LEVEL_XL.*OPTIMAL')
        execute_benchmark(std_out_file, "Recovery complete level XXL", '1', SINGLE_SOCKET, 'recovery_c_level_xxl', 'BM_PEAR_RECOVERY_SZ_TAXI_COMPLETE_LEVEL_XXL.*OPTIMAL')
        # execute_benchmark(std_out_file, "MAX_PMEM_LEVEL Insert", '1', SINGLE_SOCKET, 'insert_basic', 'BM_INSERT_SZ_TAXI_MAX_PMEM_LEVEL_M_.*threads:(1|2|4|8|9)$')
        execute_benchmark(std_out_file, "MAX_PMEM_LEVEL XL Insert", '0,1', TWO_SOCKET, 'insert_basic_xl', 'BM_INSERT_SZ_TAXI_MAX_PMEM_LEVEL_XL.*threads:(9|16|18)$')
        execute_benchmark(std_out_file, "MAX_PMEM_LEVEL XXL Insert", '0,1', TWO_SOCKET_HYPERTHREADING, 'insert_basic_xxl', 'BM_INSERT_SZ_TAXI_MAX_PMEM_LEVEL_XXL.*threads:(18|24|32|36)$')
        #
        # execute_benchmark(std_out_file, "MAX_DRAM_LEVELS Insert", '1', SINGLE_SOCKET, 'insert_strat_1', 'BM_INSERT_SZ_TAXI_MAX_DRAM_LEVELS_M_.*threads:(1|2|4|8|9)$')
        # execute_benchmark(std_out_file, "MAX_DRAM_LEVELS XL Insert", '0,1', TWO_SOCKET, 'insert_strat_1_xl', 'BM_INSERT_SZ_TAXI_MAX_DRAM_LEVELS_XL.*threads:(9|16|18)$')
        execute_benchmark(std_out_file, "MAX_DRAM_LEVELS XL Insert", '0,1', TWO_SOCKET, 'insert_strat_1_xl', 'BM_INSERT_SZ_TAXI_MAX_DRAM_LEVELS_XL.*threads:(9|18)$')
        # execute_benchmark(std_out_file, "MAX_DRAM_LEVELS XXL Insert", '0,1', TWO_SOCKET_HYPERTHREADING, 'insert_strat_1_xxl', 'BM_INSERT_SZ_TAXI_MAX_DRAM_LEVELS_XXL.*threads:(18|24|32|36)$')
        execute_benchmark(std_out_file, "MAX_DRAM_LEVELS XXL Insert", '0,1', TWO_SOCKET_HYPERTHREADING, 'insert_strat_1_xxl', 'BM_INSERT_SZ_TAXI_MAX_DRAM_LEVELS_XXL.*threads:(9|18|36)$')
        #
        # execute_benchmark(std_out_file, "MAX_DRAM_NODES Insert", '1', SINGLE_SOCKET, 'insert_strat_2', 'BM_INSERT_SZ_TAXI_MAX_DRAM_NODES_M_.*threads:(1|2|4|8|9)$')
        execute_benchmark(std_out_file, "MAX_DRAM_NODES XL Insert", '0,1', TWO_SOCKET, 'insert_strat_2_xl', 'BM_INSERT_SZ_TAXI_MAX_DRAM_NODES_XL.*threads:(9|16|18)$')
        # execute_benchmark(std_out_file, "MAX_DRAM_NODES XXL Insert", '0,1', TWO_SOCKET_HYPERTHREADING, 'insert_strat_2_xxl', 'BM_INSERT_SZ_TAXI_MAX_DRAM_NODES_XXL.*threads:(18|24|32|36)$')
        execute_benchmark(std_out_file, "MAX_DRAM_NODES XXL Insert", '0,1', TWO_SOCKET_HYPERTHREADING, 'insert_strat_2_xxl', 'BM_INSERT_SZ_TAXI_MAX_DRAM_NODES_XXL.*threads:(9|18|36)$')

        # execute_benchmark(std_out_file, "MAX_DRAM_NODES_BACKGROUND Insert", '1', SINGLE_SOCKET, 'insert_strat_2_bp', 'BM_INSERT_SZ_TAXI_MAX_DRAM_NODES_BACKGROUND_M_.*threads:(1|2|4|8|9)$')
        # execute_benchmark(std_out_file, "MAX_DRAM_NODES_BACKGROUND XXL Insert", '0,1', TWO_SOCKET_HYPERTHREADING, 'insert_strat_2_bp_xxl', 'BM_INSERT_SZ_TAXI_MAX_DRAM_NODES_BACKGROUND_XXL.*threads:(18|24|32|36)$')

        # execute_benchmark(std_out_file, "BOOST Insert", '1', SINGLE_SOCKET, 'insert_boost', 'BM_BOOST_INSERT_SZ_TAXI_')
        #
        execute_benchmark(std_out_file, "MAX_PMEM_LEVEL Query", '1', SINGLE_SOCKET, 'query_basic', 'BM_QUERY_SZ_TAXI_MAX_PMEM_LEVEL_M_.*threads:(1|2|4|8|9)$')
        # execute_benchmark(std_out_file, "MAX_PMEM_LEVEL Query max node size", '1', SINGLE_SOCKET, 'query_basic_max', 'BM_QUERY_SZ_TAXI_MAX_PMEM_LEVEL_M_.*_MAX_NODE_SIZE.*threads:(1|2|4|8|9)$')
        # execute_benchmark(std_out_file, "MAX_PMEM_LEVEL Query small node size", '1', SINGLE_SOCKET, 'query_basic_small', 'BM_QUERY_SZ_TAXI_MAX_PMEM_LEVEL_M_.*_SMALL_NODE_SIZE.*threads:(1|2|4|8|9)$')
        execute_benchmark(std_out_file, "MAX_PMEM_LEVEL XL Query", '0,1', TWO_SOCKET, 'query_basic_xl', 'BM_QUERY_SZ_TAXI_MAX_PMEM_LEVEL_XL.*threads:(9|16|18)$')
        # execute_benchmark(std_out_file, "MAX_PMEM_LEVEL XL Query max node size", '0,1', TWO_SOCKET, 'query_basic_xl_max', 'BM_QUERY_SZ_TAXI_MAX_PMEM_LEVEL_XL.*_MAX_NODE_SIZE.*threads:(9|16|18)$')
        # execute_benchmark(std_out_file, "MAX_PMEM_LEVEL XL Query small node size", '0,1', TWO_SOCKET, 'query_basic_xl_small', 'BM_QUERY_SZ_TAXI_MAX_PMEM_LEVEL_XL.*_SMALL_NODE_SIZE.*threads:(9|16|18)$')

        execute_benchmark(std_out_file, "BOOST Query", '1', SINGLE_SOCKET, 'query_boost', 'BM_BOOST_QUERY_SZ_TAXI_')

        # execute_benchmark(std_out_file, "Recovery complete level (small node size)", '1', SINGLE_SOCKET, 'recovery_c_level_small', 'BM_PEAR_RECOVERY_SZ_TAXI_COMPLETE_LEVEL_M.*SMALL')
        # execute_benchmark(std_out_file, "Recovery complete level (max node size)", '1', SINGLE_SOCKET, 'recovery_c_level_max', 'BM_PEAR_RECOVERY_SZ_TAXI_COMPLETE_LEVEL_M.*MAX')

        # execute_benchmark(std_out_file, "Recovery partial level XL", '1', SINGLE_SOCKET, 'recovery_p_level_xl', 'BM_PEAR_RECOVERY_SZ_TAXI_PARTIAL_LEVEL_XL.*OPTIMAL')
        # execute_benchmark(std_out_file, "Recovery partial level XL (small node size)", '1', SINGLE_SOCKET, 'recovery_p_level_xl_small', 'BM_PEAR_RECOVERY_SZ_TAXI_PARTIAL_LEVEL_XL.*SMALL')
        # execute_benchmark(std_out_file, "Recovery partial level XL (max node size)", '1', SINGLE_SOCKET, 'recovery_p_level_xl_max', 'BM_PEAR_RECOVERY_SZ_TAXI_PARTIAL_LEVEL_XL.*MAX')

        # execute_benchmark(std_out_file, "Recovery partial level XXL", '1', SINGLE_SOCKET, 'recovery_p_level_xxl', 'BM_PEAR_RECOVERY_SZ_TAXI_PARTIAL_LEVEL_XXL.*OPTIMAL')
        # execute_benchmark(std_out_file, "Recovery complete level XL (small node size)", '1', SINGLE_SOCKET, 'recovery_c_level_xl_small', 'BM_PEAR_RECOVERY_SZ_TAXI_COMPLETE_LEVEL_XL.*SMALL')
        # execute_benchmark(std_out_file, "Recovery complete level XL (max node size)", '1', SINGLE_SOCKET, 'recovery_c_level_xl_max', 'BM_PEAR_RECOVERY_SZ_TAXI_COMPLETE_LEVEL_XL.*MAX')

        # execute_benchmark(std_out_file, "Recovery partial level XXL (small node size)", '1', SINGLE_SOCKET, 'recovery_p_level_xxl_small', 'BM_PEAR_RECOVERY_SZ_TAXI_PARTIAL_LEVEL_XXL.*SMALL')
        # execute_benchmark(std_out_file, "Recovery partial level XXL (max node size)", '1', SINGLE_SOCKET, 'recovery_p_level_xxl_max', 'BM_PEAR_RECOVERY_SZ_TAXI_PARTIAL_LEVEL_XXL.*MAX')

        # execute_benchmark(std_out_file, "Recovery complete level XXL (small node size)", '1', SINGLE_SOCKET, 'recovery_c_level_xxl_small', 'BM_PEAR_RECOVERY_SZ_TAXI_COMPLETE_LEVEL_XXL.*SMALL')
        # execute_benchmark(std_out_file, "Recovery complete level XXL (max node size)", '1', SINGLE_SOCKET, 'recovery_c_level_xxl_max', 'BM_PEAR_RECOVERY_SZ_TAXI_COMPLETE_LEVEL_XXL.*MAX')
        execute_benchmark(std_out_file, "MAX_DRAM_NODES_BACKGROUND XL Insert", '0,1', TWO_SOCKET, 'insert_strat_2_bp_xl', 'BM_INSERT_SZ_TAXI_MAX_DRAM_NODES_BACKGROUND_XL.*threads:(9|16|18)$')
        execute_benchmark(std_out_file, "MAX_DRAM_NODES_BACKGROUND XXL Insert", '0,1', TWO_SOCKET_HYPERTHREADING, 'insert_strat_2_bp_xxl', 'BM_INSERT_SZ_TAXI_MAX_DRAM_NODES_BACKGROUND_XXL.*threads:(9|18|36)$')




