## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# EXECUTOR
######################################################################

check_PROGRAMS += gc_basic_test

executor_tests_common= 	executor/executor_tests_util.cpp \
						harness.cpp

gc_basic_test_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_basic_test.cpp