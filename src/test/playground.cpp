#include <unistd.h>
#include <memory>
#include <queue>
#include <fcntl.h>
#include <per_tree_allocator.hpp>

//#include "print.hpp"
//#include "export.hpp"
//#include "test_helpers.hpp"
#include "../benchmarks/micro_benchmark.hpp"

//template<class RTree>
//void executeBuildTest() {
//    RTree test = RTree();
//    test.insert(std::make_shared<Rectangle>(0, 2, 1, 3), 1L, <#initializer#>);
//    test.insert(std::make_shared<Rectangle>(2, 1, 3, 2), 2L, <#initializer#>);
//    print_tree<BasicRTree>(test);
//    test.insert(std::make_shared<Rectangle>(3, 2, 4, 3), 3L, <#initializer#>);
//    print_tree<BasicRTree>(test);
//    test.insert(std::make_shared<Rectangle>(1, 1, 4, 3), 4L, <#initializer#>);
//    print_tree<BasicRTree>(test);
//    test.insert(std::make_shared<Rectangle>(-1, 7, 2, 9), 5L, <#initializer#>);
//    print_tree<BasicRTree>(test);
//    test.insert(std::make_shared<Rectangle>(3, 5, 4, 6), 6L, <#initializer#>);
//    print_tree<BasicRTree>(test);
//    test.insert(std::make_shared<Rectangle>(3, 5, 4, 6), 7L, <#initializer#>);
//    print_tree<BasicRTree>(test);
//    test_tree_MBR_fit<BasicRTree>(test);
//    exportTree(test);
//}
//
//
//template<class RTree>
//void executeQueryTest() {
//    RTree test = RTree();
//    test.insert(std::make_shared<Rectangle>(0, 2, 1, 3), 1L, <#initializer#>);
//    exportTree(test, "1_");
//    test.insert(std::make_shared<Rectangle>(2, 1, 3, 2), 2L, <#initializer#>);
//    exportTree(test, "2_");
//    test.insert(std::make_shared<Rectangle>(3, 2, 4, 3), 3L, <#initializer#>);
//    exportTree(test, "3_");
//    test.insert(std::make_shared<Rectangle>(1, 1, 4, 3), 4L, <#initializer#>);
//    exportTree(test, "4_");
//    test.insert(std::make_shared<Rectangle>(-1, 7, 2, 9), 5L, <#initializer#>);
//    exportTree(test, "5_");
//    test.insert(std::make_shared<Rectangle>(3, 5, 4, 6), 6L, <#initializer#>);
//    exportTree(test, "6_");
//    test.insert(std::make_shared<Rectangle>(4, 5, 5, 6), 7L, <#initializer#>);
//    print_tree<BasicRTree>(test);
//    test_tree_MBR_fit<BasicRTree>(test);
//    exportTree(test, "final_");
//    std::vector<Branch<void, long>> result;
//    std::shared_ptr<std::vector<Branch<void, long>>> result_ptr = std::make_shared<std::vector<Branch<void, long>>>(result);
//    result.insert(new Rectangle(1,2,1,2), 1);
//    test.intersect_query(std::make_shared<Rectangle>(0,1,3,3), result_ptr);
//    result.insert(new Rectangle(1,2,1,2), 1);
//}

//int main() {
////    executeQueryTest<BasicRTree>();
//    Rectangle<float>* const temp1 = micro_benchmark::get_rb_taxi_rects_10m();
//    Rectangle<float>* const temp2 = micro_benchmark::get_second_rb_taxi_rects_10m();
//
//    double avg_area_1 = 0.0;
//    double avg_area_2 = 0.0;
//    int count_sz_1 =0;
//    int count_sz_2 = 0;
//
//    for(int i = 0; i < 1000000; i++) {
//        //(avg_first_insert * (i - 1) + mid_cpu_cycles - start_1_cpu_cycles) / i;
//        const float area_1 = area_inline(temp1[i]);
//        const float area_2 = area_inline(temp2[i]);
//        if(area_1 == 0.0f) {
//            count_sz_1++;
//        } else {
//            avg_area_1 = (avg_area_1 * i + area_1) / (i + 1);
//        }
//        if(area_2 == 0.0f) {
//            count_sz_2++;
//        } else {
//            avg_area_2 = (avg_area_2 * i + area_2) / (i +1);
//        }
//    }
//    std::cout << "Avg area 1: " << avg_area_1 << " (count sz: " << count_sz_1 << ")" << std::endl;
//    std::cout << "Avg area 2: " << avg_area_2 << " (count sz: " << count_sz_2 << ")" << std::endl;
//    return 0;
//}
//
int main() {
    int _dev_dax_fd;
    if ((_dev_dax_fd = open("/dev/dax1.0", O_RDWR)) < 0) {
        perror("open");
        exit(1);
    }
//    void* temp = mmap(NULL,(size_t)(1024l*1024l*1024l*20l),PROT_READ|PROT_WRITE,MAP_PRIVATE | MAP_ANON,_dev_dax_fd,0);
//    if((long)temp == -1) {
//        std::cerr << "Error with mmap: " << strerror(errno) <<std::endl;
//        exit(1);
//    }
    char* temp = (char*)aligned_alloc(CACHE_LINE_SIZE,ALLIGN_TO_SIZE(NODE_POOL_SIZE/5, CACHE_LINE_SIZE));
    char* new_temp = temp + (NODE_POOL_SIZE)+1000;
    *new_temp = 'c';
    usleep(1000000*10);
    // Result: mmaped space is not displayed in numactl
    return 0;
}
