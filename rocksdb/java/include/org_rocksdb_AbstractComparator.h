/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_rocksdb_AbstractComparator */

#ifndef _Included_org_rocksdb_AbstractComparator
#define _Included_org_rocksdb_AbstractComparator
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_rocksdb_AbstractComparator
 * Method:    usingDirectBuffers
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_rocksdb_AbstractComparator_usingDirectBuffers
  (JNIEnv *, jobject, jlong);

/*
 * Class:     org_rocksdb_AbstractComparator
 * Method:    createNewComparator
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_AbstractComparator_createNewComparator
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif