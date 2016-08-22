package com.gigaspaces.internal.query.explain_plan;

import com.gigaspaces.internal.metadata.EntryTypeDesc;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.server.storage.EntryDataType;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.TemplateEntryData;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.util.Map;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class TemplateEntryDataMock extends TemplateEntryData{
    private ICustomQuery _customQuery;
    private Object[] _fieldsValues;
    private short[] _extendedMatchCodes;


    public TemplateEntryDataMock(ICustomQuery _customQuery, Object[] _fieldsValues, short[] _extendedMatchCodes) {
        this._customQuery = _customQuery;
        this._fieldsValues = _fieldsValues;
        this._extendedMatchCodes = _extendedMatchCodes;
    }

    public ICustomQuery getCustomQuery() {
        return _customQuery;
    }

    @Override
    public EntryDataType getEntryDataType() {
        return null;
    }

    @Override
    public EntryTypeDesc getEntryTypeDesc() {
        return null;
    }

    @Override
    public int getNumOfFixedProperties() {
        return 0;
    }

    @Override
    public void setFixedPropertyValue(int index, Object value) {

    }

    @Override
    public void setFixedPropertyValues(Object[] values) {

    }

    public Object[] getFixedPropertiesValues() {
        return _fieldsValues;
    }

    @Override
    public Map<String, Object> getDynamicProperties() {
        return null;
    }

    @Override
    public void setDynamicPropertyValue(String propertyName, Object value) {

    }

    @Override
    public void unsetDynamicPropertyValue(String propertyName) {

    }

    @Override
    public void setDynamicProperties(Map<String, Object> dynamicProperties) {

    }

    @Override
    public long getTimeToLive(boolean useDummyIfRelevant) {
        return 0;
    }

    public short[] getExtendedMatchCodes() {
        return _extendedMatchCodes;
    }

    @Override
    public SpaceTypeDescriptor getSpaceTypeDescriptor() {
        return null;
    }

    @Override
    public Object getFixedPropertyValue(int position) {
        return null;
    }

    @Override
    public Object getPropertyValue(String name) {
        return null;
    }

    @Override
    public Object getPathValue(String path) {
        return null;
    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public long getExpirationTime() {
        return 0;
    }
}
