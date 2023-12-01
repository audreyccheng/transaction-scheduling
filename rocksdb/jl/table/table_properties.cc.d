jl/table/table_properties.cc.d table/table_properties.o \
  jl/table/table_properties.o: table/table_properties.cc \
  include/rocksdb/table_properties.h include/rocksdb/customizable.h \
  include/rocksdb/configurable.h include/rocksdb/rocksdb_namespace.h \
  include/rocksdb/status.h include/rocksdb/slice.h \
  include/rocksdb/cleanable.h include/rocksdb/types.h \
  db/seqno_to_time_mapping.h port/malloc.h port/port.h port/port_posix.h \
  include/rocksdb/port_defs.h include/rocksdb/env.h \
  include/rocksdb/functor_wrapper.h include/rocksdb/thread_status.h \
  include/rocksdb/unique_id.h table/table_properties_internal.h \
  table/unique_id_impl.h util/random.h util/string_util.h
