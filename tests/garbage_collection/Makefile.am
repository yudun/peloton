## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# EXECUTOR
######################################################################

check_PROGRAMS += gc_update_test gc_delete_test gc_stress_test

executor_tests_common= 	executor/executor_tests_util.cpp \
						harness.cpp

gc_update_test_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_update_test.cpp

gc_delete_test_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_delete_test.cpp

gc_stress_test_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_stress_test.cpp