/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_rocksdb_ColumnFamilyHandle */

#ifndef _Included_org_rocksdb_ColumnFamilyHandle
#define _Included_org_rocksdb_ColumnFamilyHandle
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    getName
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_rocksdb_ColumnFamilyHandle_getName
  (JNIEnv *, jobject, jlong);

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    getID
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_rocksdb_ColumnFamilyHandle_getID
  (JNIEnv *, jobject, jlong);

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    getDescriptor
 * Signature: (J)Lorg/rocksdb/ColumnFamilyDescriptor;
 */
JNIEXPORT jobject JNICALL Java_org_rocksdb_ColumnFamilyHandle_getDescriptor
  (JNIEnv *, jobject, jlong);

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    disposeInternal
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_ColumnFamilyHandle_disposeInternal
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif