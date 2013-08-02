package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comparison relations for text objects.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Text implements Relation {

    /**
     * Whether the text contains a given term
     */
    CONTAINS {

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
            Preconditions.checkArgument(isValidCondition(condition),"Invalid condition provided: %s",condition);
            if (value==null) return false;
            if (!(value instanceof String)) log.debug("Value not a string: " + value);
            return value.toString().contains((String)condition);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            if (condition==null) return false;
            else if (condition instanceof String && StringUtils.isNotBlank((String)condition)) return true;
            else return false;
        }
    },

    /**
     * Whether the text starts with a given term
     */
    PREFIX {

        @Override
        public boolean satisfiesCondition(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof String);
            if (value==null) return false;
            if (!(value instanceof String)) log.debug("Value not a string: " + value);
            return value.toString().startsWith((String)condition);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition!=null && condition instanceof String;
        }

    },
	
	/**
	 * Whether the text matches the given regular expression
	 */
	REGEXP {
	
	    @Override
        public boolean satisfiesCondition(Object value, Object condition) {
            Preconditions.checkArgument(condition instanceof String);
            if (value==null) return false;
            if (!(value instanceof String)) log.debug("Value not a string: " + value);
            return value.toString().startsWith((String)condition);
        }

        @Override
        public boolean isValidCondition(Object condition) {
			if (condition==null) return false;
            else if (condition instanceof String && StringUtils.isNotBlank((String)condition)) return true;
            else return false;
        }
	};

    private static final Logger log = LoggerFactory.getLogger(Text.class);



    @Override
    public boolean isValidDataType(Class<?> clazz) {
        Preconditions.checkNotNull(clazz);
        return clazz.equals(String.class);
    }


}
