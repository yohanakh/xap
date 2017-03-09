package com.j_spaces.core.cache;

/**
 * Created by yechiel on 3/5/17.
 */
public enum EntriesIterScanType {
        ALL,
        TRANSIENT_ONLY,
        BLOBSTORE_ONLY,
        MEMORY_ONLY /*including blobstore + transient+ db in memory*/,
        DB_ONLY, /*db inc those in lru' */
        DB_DISK_ONLY  /*db direct iter to disk*/
}
