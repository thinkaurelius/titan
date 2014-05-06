package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.util.encoding.StringEncoding;

import java.util.EnumMap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum EntryMetaData {

    TTL, VISIBILITY, TIMESTAMP;


    public static final java.util.Map<EntryMetaData,Object> EMPTY_METADATA = ImmutableMap.of();

    public Class getDataType() {
        switch(this) {
            case VISIBILITY: return String.class;
            case TTL:
            case TIMESTAMP: return Long.class;
            default: throw new AssertionError("Unexpected meta data: " + this);
        }
    }

    public boolean isValidData(Object data) {
        Preconditions.checkNotNull(data);
        switch(this) {
            case VISIBILITY:
                if (!(data instanceof String)) return false;
                return StringEncoding.isAsciiString((String)data);
            case TTL:
            case TIMESTAMP:
                return data instanceof Long;
            default: throw new AssertionError("Unexpected meta data: " + this);
        }
    }

    public static class Map extends EnumMap<EntryMetaData,Object> {

        public Map() {
            super(EntryMetaData.class);
        }

        @Override
        public Object put(EntryMetaData key, Object value) {
            Preconditions.checkArgument(key.isValidData(value),"Invalid meta data [%s] for [%s]",value,key);
            return super.put(key,value);
        }

    }

}