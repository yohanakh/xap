package com.gigaspaces.internal.server.space;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.DirectPersistencyAttributeStoreException;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.DirectPersistencyRecoveryException;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.j_spaces.core.Constants.DirectPersistency.ZOOKEEPER.ATTRIBUET_STORE_HANDLER_CLASS_NAME;

/**
 * Created by tamirs
 * on 5/9/17.
 */
public class ZookeeperLastPrimaryHandler {

    @SuppressWarnings("FieldCanBeLocal")
    private final String LAST_PRIMARY_PATH_PROPERTY = "com.gs.blobstore.zookeeper.lastprimarypath";
    public static final String LAST_PRIMARY_ZOOKEEPER_PATH_DEFAULT = "/last_primary";

    private final SpaceImpl _spaceImpl;
    @SuppressWarnings("FieldCanBeLocal")
    private final boolean isMemoryXtendSpace;

    private final String _attributeStoreKey;
    private final Logger _logger;
    private String attributeStoreValue;

    private final AttributeStore _attributeStore;

    public ZookeeperLastPrimaryHandler(SpaceImpl spaceImpl, boolean isMemoryXtendSpace, Logger logger) {
        this._spaceImpl = spaceImpl;
        this.isMemoryXtendSpace = isMemoryXtendSpace;
        this._attributeStoreKey = spaceImpl.getEngine().getSpaceName() + "." + spaceImpl.getEngine().getPartitionIdOneBased() + ".primary";
        this._logger = logger;

        this.attributeStoreValue = _spaceImpl.getEngine().getFullSpaceName();
        if(!isMemoryXtendSpace){
            this.attributeStoreValue += "#_#" + _spaceImpl.getSpaceUuid().toString();
        }
        String lastPrimaryZookeepertPath = System.getProperty(LAST_PRIMARY_PATH_PROPERTY, LAST_PRIMARY_ZOOKEEPER_PATH_DEFAULT);
        this._attributeStore = createZooKeeperAttributeStore(lastPrimaryZookeepertPath);
    }


    private AttributeStore createZooKeeperAttributeStore(String lastPrimaryPath) {
        int connectionTimeout = _spaceImpl.getConfig().getZookeeperConnectionTimeout();
        int sessionTimeout = _spaceImpl.getConfig().getZookeeperSessionTimeout();
        int retryTimeout = _spaceImpl.getConfig().getZookeeperRetryTimeout();
        int retryInterval = _spaceImpl.getConfig().getZookeeperRetryInterval();

        final Constructor constructor;
        try {
            //noinspection unchecked
            constructor = ClassLoaderHelper.loadLocalClass(ATTRIBUET_STORE_HANDLER_CLASS_NAME)
                    .getConstructor(String.class, int.class, int.class, int.class, int.class);
            return (AttributeStore) constructor.newInstance(lastPrimaryPath, sessionTimeout, connectionTimeout, retryTimeout, retryInterval);
        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE))
                _logger.log(Level.SEVERE, "Failed to create attribute store ");
            throw new DirectPersistencyRecoveryException("Failed to start [" + (_spaceImpl.getEngine().getFullSpaceName())
                    + "] Failed to create attribute store.");
        }
    }

    public void removeLastPrimaryRecord() throws IOException {
        _attributeStore.remove(_attributeStoreKey);
    }

    public void setMeAsLastPrimary() throws IOException {
        String previousLastPrimary = _attributeStore.set(_attributeStoreKey, attributeStoreValue);
        if (_logger.isLoggable(Level.INFO))
            _logger.log(Level.INFO, "Set as last primary ["+ attributeStoreValue +"], previous last primary is ["+previousLastPrimary+"]");
    }

    public boolean isMeLastPrimary() {
        try {
            return attributeStoreValue.equals(getLastPrimaryName());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public String getLastPrimaryName() throws IOException {
        return _attributeStore.get(_attributeStoreKey);
    }

}
