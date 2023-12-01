/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_rocksdb_CompactionJobStats */

#ifndef _Included_org_rocksdb_CompactionJobStats
#define _Included_org_rocksdb_CompactionJobStats
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    newCompactionJobStats
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_newCompactionJobStats
  (JNIEnv *, jclass);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    disposeInternal
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_CompactionJobStats_disposeInternal
  (JNIEnv *, jobject, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    reset
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_CompactionJobStats_reset
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    add
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_CompactionJobStats_add
  (JNIEnv *, jclass, jlong, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    elapsedMicros
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_elapsedMicros
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numInputRecords
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_numInputRecords
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numInputFiles
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_numInputFiles
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numInputFilesAtOutputLevel
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_numInputFilesAtOutputLevel
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numOutputRecords
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_numOutputRecords
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numOutputFiles
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_numOutputFiles
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    isManualCompaction
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_rocksdb_CompactionJobStats_isManualCompaction
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    totalInputBytes
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_totalInputBytes
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    totalOutputBytes
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_totalOutputBytes
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numRecordsReplaced
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_numRecordsReplaced
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    totalInputRawKeyBytes
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_totalInputRawKeyBytes
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    totalInputRawValueBytes
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_totalInputRawValueBytes
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numInputDeletionRecords
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_numInputDeletionRecords
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numExpiredDeletionRecords
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_numExpiredDeletionRecords
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numCorruptKeys
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_numCorruptKeys
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    fileWriteNanos
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_fileWriteNanos
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    fileRangeSyncNanos
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_fileRangeSyncNanos
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    fileFsyncNanos
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_fileFsyncNanos
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    filePrepareWriteNanos
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_filePrepareWriteNanos
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    smallestOutputKeyPrefix
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_rocksdb_CompactionJobStats_smallestOutputKeyPrefix
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    largestOutputKeyPrefix
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_rocksdb_CompactionJobStats_largestOutputKeyPrefix
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numSingleDelFallthru
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_numSingleDelFallthru
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numSingleDelMismatch
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_CompactionJobStats_numSingleDelMismatch
  (JNIEnv *, jclass, jlong);

#ifdef __cplusplus
}
#endif
#endif