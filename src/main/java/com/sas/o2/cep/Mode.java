package com.sas.o2.cep;

/**
 * Mode for inserting publishing data to esp engine. The enum is mapped to a
 * string-opcode that will be used as a prefix for each event published.
 *
 */
public enum Mode {
    /**
     * inserts given data.
     */
    insert("i,n,"),
    /**
     * updates/ inserts given data.
     */
    upsert("p,n,"),
    /**
     * inserts given data and adds a uuid at each item.
     */
    insertAddId("i,n,"),
    /**
     * deletes given data.
     */
    delete("d,n,"),
    /**
     * no action is specified. Opcode must be set (added) on each event. Use
     * opcodes (prefix) given by other {@link Mode}s.
     */
    dynamic("");

    /**
     * Prefix/ opcode for cep engine.
     */
    private String prefix;

    /**
     *
     * @param prefix
     *            opcode for cep engine
     */
    private Mode(final String prefix) {
        this.prefix = prefix;
    }

    /**
     *
     * @return prefix/opcode for this mode
     */
    public String prefix() {
        return prefix;
    }
}
