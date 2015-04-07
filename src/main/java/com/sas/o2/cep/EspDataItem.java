/**
 *
 */
package com.sas.o2.cep;

import com.sas.esp.api.server.datavar.FieldTypes;


/**
 * Represents an event in esp - its data (value) and meta data (name, type and
 * index number).
 *
 * @author moritz löser (moritz.loeser@prodyna.com)
 *
 */
public class EspDataItem {
    /**
     * Index number with in esp window.
     */
    private int index;

    /**
     * Name of field in esp.
     */
    private String name;

    /**
     * Value as string, as delivered from esp.
     */
    private String value;

    /**
     * Esp internal type of data.
     */
    private FieldTypes type;

    /**
     *
     * @param index
     * @param name
     * @param value
     * @param type
     */
    public EspDataItem(final int index, final String name, final String value, final FieldTypes type) {
        this.index = index;
        this.name = name;
        this.value = value;
        this.type = type;

    }

    /**
     * @return the index
     */
    public final int getIndex() {
        return index;
    }

    /**
     * @return the name
     */
    public final String getName() {
        return name;
    }

    /**
     * @return the value
     */
    public final String getValue() {
        return value;
    }

    /**
     * @return the type
     */
    public final FieldTypes getType() {
        return type;
    }

    @Override
    public final String toString() {
        return "DataItem: (" + index + ") " + name + "=" + value + " (type: " + type.name() + ")";
    }

}
