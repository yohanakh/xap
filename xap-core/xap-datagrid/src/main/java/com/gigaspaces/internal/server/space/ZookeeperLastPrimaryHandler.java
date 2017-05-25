package com.gigaspaces.internal.server.space;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.DirectPersistencyRecoveryException;
import com.j_spaces.core.admin.SpaceConfig;
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
    private final String separator = "#_#";
    private final Logger _logger;

    private final SpaceImpl _spaceImpl;
    private final String _attributeStoreKey;
    private String attributeStoreValue;

    private final AttributeStore _attributeStore;
    private String lastPrimaryPath;

    public ZookeeperLastPrimaryHandler(SpaceImpl spaceImpl, Logger logger) {
        this._logger = logger;
        this._spaceImpl = spaceImpl;
        this._attributeStoreKey = spaceImpl.getName() + "." + spaceImpl.getPartitionIdOneBased() + ".primary";
        this.attributeStoreValue = spaceImpl.getInstanceId() + separator + spaceImpl.getSpaceUuid().toString();
        this._attributeStore = createZooKeeperAttributeStore();
    }

    private AttributeStore createZooKeeperAttributeStore() {
        this.lastPrimaryPath = System.getProperty(LAST_PRIMARY_PATH_PROPERTY, LAST_PRIMARY_ZOOKEEPER_PATH_DEFAULT);
        try {
            //noinspection unchecked
            Constructor constructor = ClassLoaderHelper.loadLocalClass(ATTRIBUET_STORE_HANDLER_CLASS_NAME)
                    .getConstructor(String.class, SpaceConfig.class);
            return (AttributeStore) constructor.newInstance(lastPrimaryPath, _spaceImpl.getConfig());
        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE))
                _logger.log(Level.SEVERE, "Failed to create attribute store ");
            throw new DirectPersistencyRecoveryException("Failed to start [" + (_spaceImpl.getServiceName())
                    + "] Failed to create attribute store.");
        }
    }

    public void removeLastPrimaryRecord() throws IOException {
        _logger.info("Removing key ["+_attributeStoreKey+"] from ZK");
        _attributeStore.remove(_attributeStoreKey);
    }

    public void setMeAsLastPrimary() throws IOException {
        String previousLastPrimary = _attributeStore.set(_attributeStoreKey, attributeStoreValue);
        if (_logger.isLoggable(Level.INFO))
            _logger.log(Level.INFO, "Set as last primary ["+ attributeStoreValue +"] for key ["+_attributeStoreKey+"] in ZK. Previous last primary is ["+previousLastPrimary+"]");
    }

    public boolean isMeLastPrimary() {
        try {
            return attributeStoreValue.equals(getLastPrimaryName());
        } catch (IOException e) {
            _logger.log(Level.WARNING, "Failed to get last primary from ZK", e);
            return false;
        }
    }

    public String getLastPrimaryName() throws IOException {
        return _attributeStore.get(_attributeStoreKey);
    }

    public String getLastPrimaryNameMemoryXtend() throws IOException {
        String lastPrimary = this._attributeStore.get(_attributeStoreKey);
        if(lastPrimary == null) {
            return null;
        }
        else {
            String[] split = lastPrimary.split(separator);
            if(split.length == 2) {
                return split[0];
            } else {
                _logger.log(Level.WARNING, "Got unrecognized last primary record [" + lastPrimary + "]. Should be <instance_id>" + separator + "<service_id> ");
                return null;
            }
        }
    }

    public String getAttributeStoreKey() {
        return _attributeStoreKey;
    }

    public String getLastPrimaryPath() {
        return lastPrimaryPath;
    }

    public String getSeparator() {
        return separator;
    }
}
