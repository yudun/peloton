## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# EXECUTOR
######################################################################

check_PROGRAMS += gc_update_test_vacuum gc_update_test_coop gc_update_test_epoch \
				gc_delete_test_vacuum gc_delete_test_coop gc_delete_test_epoch \
				gc_stress_test_vacuum gc_stress_test_coop gc_stress_test_epoch

executor_tests_common= 	executor/executor_tests_util.cpp \
						harness.cpp

gc_update_test_vacuum_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_update_test_vacuum.cpp

gc_update_test_coop_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_update_test_coop.cpp

gc_update_test_epoch_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_update_test_epoch.cpp

gc_delete_test_vacuum_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_delete_test_vacuum.cpp

gc_delete_test_coop_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_delete_test_coop.cpp

gc_delete_test_epoch_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_delete_test_epoch.cpp

gc_stress_test_vacuum_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_stress_test_vacuum.cpp


gc_stress_test_coop_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_stress_test_coop.cpp


gc_stress_test_epoch_SOURCES = \
						$(executor_tests_common) \
						garbage_collection/gc_stress_test_epoch.cpp