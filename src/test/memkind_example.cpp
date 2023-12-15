// SPDX-License-Identifier: BSD-3-Clause
/* Copyright (C) 2015 - 2020 Intel Corporation. */

#include <memkind.h>

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>

#define PMEM_MAX_SIZE_EXAMPLE (1024 * 1024 * 32)

static char path[PATH_MAX]="/mnt/pmem/";

int basic_example(int argc, char *const *argv);
int jemalloc_meta_data_test(int argc, char *const *argv);

static void print_err_message(int err)
{
    char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
    memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
    fprintf(stderr, "%s\n", error_message);
}

int main(int argc, char *argv[])
{
    return basic_example(argc, argv);

}

int jemalloc_meta_data_test(int argc, char *const *argv) {
    struct memkind *pmem_kind = NULL;
    int err = 0;

    if (argc > 2) {
        fprintf(stderr, "Usage: %s [pmem_kind_dir_path]\n", argv[0]);
        return 1;
    } else if (argc == 2 && (realpath(argv[1], path) == NULL)) {
        fprintf(stderr, "Incorrect pmem_kind_dir_path %s\n", argv[1]);
        return 1;
    }

    int pmem_size;
    if(argc == 3 && atoi(argv[2]) > 0) {
        pmem_size = atoi(argv[2]);
    } else {
        pmem_size = PMEM_MAX_SIZE_EXAMPLE;
    }

    fprintf(stdout,
            "This example shows how to allocate memory and possibility to exceed pmem kind size."
            "\nPMEM kind directory: %s\n", path);

    int status = memkind_check_dax_path(path);
    if (!status) {
        fprintf(stdout, "PMEM kind %s is on DAX-enabled file system.\n", path);
    } else {
        fprintf(stdout, "PMEM kind %s is not on DAX-enabled file system.\n", path);
    }

    // Create PMEM partition with specific size
    err = memkind_create_pmem(path, pmem_size, &pmem_kind);
    if (err) {
        print_err_message(err);
        return 1;
    }

    int step_length = 4;
    int i = 0;
    char* ptr = (char *)memkind_malloc(pmem_kind, step_length);
    while (ptr != NULL) {
        ptr = (char *)memkind_malloc(pmem_kind, step_length);
        i += step_length;
    }

    std::cout << "Jemalloc meta data size " << (pmem_size-i)/1024 << "KB from " << pmem_size/1024 << "KB pool size" << std::endl;

    return 0;
}

int basic_example(int argc, char *const *argv) {
    struct memkind *pmem_kind = NULL;
    int err = 0;

    if (argc > 2) {
        fprintf(stderr, "Usage: %s [pmem_kind_dir_path]\n", argv[0]);
        return 1;
    } else if (argc == 2 && (realpath(argv[1], path) == NULL)) {
        fprintf(stderr, "Incorrect pmem_kind_dir_path %s\n", argv[1]);
        return 1;
    }

    fprintf(stdout,
            "This example shows how to allocate memory and possibility to exceed pmem kind size."
            "\nPMEM kind directory: %s\n", path);

    int status = memkind_check_dax_path(path);
    if (!status) {
        fprintf(stdout, "PMEM kind %s is on DAX-enabled file system.\n", path);
    } else {
        fprintf(stdout, "PMEM kind %s is not on DAX-enabled file system.\n", path);
    }

    // Create PMEM partition with specific size
    err = memkind_create_pmem(path, PMEM_MAX_SIZE_EXAMPLE, &pmem_kind);
    if (err) {
        print_err_message(err);
        return 1;
    }

//    size_t v1 = -3;
//    size_t v2 = -3;
//    size_t v3 = -3;
//    size_t v4 = -3;
//
//    memkind_update_cached_stats();
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_RESIDENT, &v1);
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_ACTIVE, &v2);
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_ALLOCATED, &v3);
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_MAX_VALUE, &v4);

    char *pmem_str1 = NULL;
    char *pmem_str2 = NULL;
    char *pmem_str3 = NULL;
    char *pmem_str4 = NULL;

    // Allocate 512 Bytes of 32 MB available --- if left out 4096B more
    pmem_str1 = (char *)memkind_malloc(pmem_kind, 512);
    if (pmem_str1 == NULL) {
        fprintf(stderr, "Unable to allocate pmem string (pmem_str1).\n");
        return 1;
    }
//    memkind_update_cached_stats();
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_RESIDENT, &v1);
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_ACTIVE, &v2);
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_ALLOCATED, &v3);
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_MAX_VALUE, &v4);
//    usleep(10000000);
// Allocate 8 MB of 31.9 MB available --- if left out 8.392.704B more
    pmem_str2 = (char *)memkind_malloc(pmem_kind, 8 * 1024 * 1024);
    if (pmem_str2 == NULL) {
        fprintf(stderr, "Unable to allocate pmem string (pmem_str2).\n");
        return 1;
    }
//    memkind_update_cached_stats();
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_RESIDENT, &v1);
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_ACTIVE, &v2);
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_ALLOCATED, &v3);
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_MAX_VALUE, &v4);
//    usleep(10000000);

    // Allocate 16 MB of 23.9 MB available --- if left out 16.781.312B more
    pmem_str3 = (char *)memkind_malloc(pmem_kind, 16 * 1024 * 1024);
    if (pmem_str3 == NULL) {
        fprintf(stderr, "Unable to allocate pmem string (pmem_str3).\n");
        return 1;
    }
//    memkind_update_cached_stats();
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_RESIDENT, &v1);
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_ACTIVE, &v2);
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_ALLOCATED, &v3);
//    memkind_get_stat(pmem_kind, MEMKIND_STAT_TYPE_MAX_VALUE, &v4);
//    usleep(10000000);

    // Allocate 16 MB of 7.9 MB available - Out Of Memory expected
    pmem_str4 = (char *)memkind_malloc(pmem_kind, (16 * 1024 * 1024));
    if (pmem_str4 != NULL) {
        fprintf(stderr,
                "Failure, this allocation should not be possible (expected result was NULL).\n");
//        return 1;
    }
//    int step_length = 8*4;
//    int i = 0;
//    char* ptr = (char *)memkind_malloc(pmem_kind, step_length);
//    while (ptr != NULL) {
//        ptr = (char *)memkind_malloc(pmem_kind, step_length);
//        i += step_length;
//    }
//
//    std::cout << "Remaining size " << i << "B from " << PMEM_MAX_SIZE_EXAMPLE/1024 << "KB pool size" << std::endl;

    sprintf(pmem_str1, "Hello world from pmem - pmem_str1.\n");
    sprintf(pmem_str2, "Hello world from pmem - pmem_str2.\n");
    sprintf(pmem_str3, "Hello world from persistent memory - pmem_str3.\n");

    fprintf(stdout, "%s", pmem_str1);
    fprintf(stdout, "%s", pmem_str2);
    fprintf(stdout, "%s", pmem_str3);

    memkind_free(pmem_kind, pmem_str1);
    memkind_free(pmem_kind, pmem_str2);
    memkind_free(pmem_kind, pmem_str3);
//
//    err = memkind_destroy_kind(pmem_kind);
//    if (err) {
//        print_err_message(err);
//        return 1;
//    }

    fprintf(stdout, "Memory was successfully allocated and released.\n");

    return 0;
}
