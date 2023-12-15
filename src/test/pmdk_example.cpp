// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2019-2020, Intel Corporation */

/*
 * basic.c -- simple example for the libpmem2
 */

#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
#ifndef _WIN32
#include <unistd.h>
#else
#include <io.h>
#endif
#include <libpmem2.h>

struct test_obj {
    int i1;
    long l1;
    float f1;

    test_obj(int i, long l, float f) : i1(i),l1(l),f1(f) {}
};

void create_and_write_obj(char* f1, char* f2, pmem2_persist_fn persist) {
    new((test_obj*)f2) test_obj(42,4242,42.42f);
    persist(f2,sizeof(test_obj));
    f2 += sizeof(test_obj);
    test_obj* t2 = new test_obj(84,8484,84.84f);
    memcpy(f2,t2, sizeof(test_obj));
    persist(f2,sizeof(test_obj));
    char* temp = f2;
    memcpy(f1,&temp,sizeof(char*));
    persist(f1,sizeof(char*));
}

void read_and_test_obj(char* f1, char* f2) {
    test_obj* t1 = (test_obj*)f2;
    f2 += sizeof(test_obj);
    test_obj* t2 = (test_obj*)f2;
    test_obj** t2_ptr = (test_obj**)f1;

}

char* initialize_mapping(char* name, struct pmem2_config *cfg, int *fd, struct pmem2_source **src, struct pmem2_map **map) {

    if ((*fd = open(name, O_RDWR)) < 0) {
        perror("open");
        exit(1);
    }

    if (pmem2_source_from_fd(src, *fd)) {
        pmem2_perror("pmem2_source_from_fd");
        exit(1);
    }

    if (pmem2_map_new(map, cfg, *src)) {
        pmem2_perror("pmem2_map_new");
        exit(1);
    }

    return (char*)pmem2_map_get_address(*map);
}

int
main(int argc, char *argv[])
{
    int* fd1 = (int*)malloc(sizeof(int));
    int* fd2 = (int*)malloc(sizeof(int));
    int* fd3 = (int*)malloc(sizeof(int));
    struct pmem2_config *cfg;
    struct pmem2_map *map1;
    struct pmem2_map *map2;
    struct pmem2_map *map3;
    struct pmem2_source *src1;
    struct pmem2_source *src2;
    struct pmem2_source *src3;
    pmem2_persist_fn persist1;
    pmem2_persist_fn persist2;
    pmem2_persist_fn persist3;

    if (argc != 5) {
        fprintf(stderr, "usage: %s -[r,w] file1 file2 save_file\n", argv[0]);
        exit(1);
    }



    if (pmem2_config_new(&cfg)) {
        pmem2_perror("pmem2_config_new");
        exit(1);
    }

    if (pmem2_config_set_required_store_granularity(cfg,
                                                    PMEM2_GRANULARITY_PAGE)) {
        pmem2_perror("pmem2_config_set_required_store_granularity");
        exit(1);
    }

    char *save_addr = initialize_mapping(argv[4],cfg,fd3,&src3,&map3);

    char *addr1;
    char *addr2;

    if(*save_addr == 42) {
        save_addr++;
        void* start_addr = *((void**)save_addr);
        struct pmem2_vm_reservation *rsv_temp;
        if (pmem2_vm_reservation_new(&rsv_temp, start_addr, 10*(1024*1024))) {
            pmem2_perror("pmem2_vm_reservation_new");
            exit(1);
        }
        pmem2_config_set_vm_reservation(cfg,rsv_temp,0);
        addr1 = initialize_mapping(argv[2],cfg,fd1,&src1,&map1);
        pmem2_config_set_vm_reservation(cfg,rsv_temp,5*(1024*1024));
        addr2 = initialize_mapping(argv[3],cfg,fd2,&src2,&map2);
    } else {
        struct pmem2_vm_reservation *rsv_temp;
        if (pmem2_vm_reservation_new(&rsv_temp, NULL, 10*(1024*1024))) {
            pmem2_perror("pmem2_vm_reservation_new");
            exit(1);
        }
        void* start_addr = pmem2_vm_reservation_get_address(rsv_temp);
        pmem2_config_set_vm_reservation(cfg,rsv_temp,0);
        addr1 = initialize_mapping(argv[2],cfg,fd1,&src1,&map1);
        pmem2_config_set_vm_reservation(cfg,rsv_temp,5*(1024*1024));
        addr2 = initialize_mapping(argv[3],cfg,fd2,&src2,&map2);
        *save_addr = 42;
        save_addr++;
        memcpy(save_addr,&start_addr,sizeof(void*));
        save_addr+= sizeof(void*);

        pmem2_get_persist_fn(map3)(save_addr, sizeof(char)+2*(sizeof(int)));
    }

    // TODO test again if one persist function works for all
    persist1 = pmem2_get_persist_fn(map1);
    persist2 = pmem2_get_persist_fn(map2);

    if (strcmp (argv[1], "-w") == 0) {
        create_and_write_obj(addr1,addr2,pmem2_get_persist_fn(map3));
    } else {
        read_and_test_obj(addr1,addr2);
    }

    pmem2_map_delete(&map1);
    pmem2_map_delete(&map2);
    pmem2_map_delete(&map3);
    pmem2_source_delete(&src1);
    pmem2_source_delete(&src2);
    pmem2_source_delete(&src3);
    pmem2_config_delete(&cfg);
    close(*fd1);
    close(*fd2);
    close(*fd3);

    return 0;
}
