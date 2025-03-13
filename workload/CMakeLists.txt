# set(DB_SOURCES smallbank_db.cc)
# set(TXN_SOURCES smallbank_txn.cc)
# set(TXN_SOURCES main.cc)

# add_library(smallbank_db STATIC ${DB_SOURCES})
# add_library(smallbank_txn STATIC ${TXN_SOURCES})

# set_target_properties(smallbank_db PROPERTIES LINKER_LANGUAGE CXX)
# set_target_properties(smallbank_txn PROPERTIES LINKER_LANGUAGE CXX)

# target_link_libraries(smallbank_db core)
# target_link_libraries(smallbank_txn core)

set(DB_SOURCES main.cc)
add_library(smallbank_db STATIC ${DB_SOURCES})
set_target_properties(smallbank_db PROPERTIES LINKER_LANGUAGE CXX)

add_executable(smallbank_txn main.cc)
target_link_libraries(smallbank_txn global)