/***
 * @author zacharycolerossman 
 * @version 7/17/18
 * 
 * Class for entries in a priority queue, sortable by the key (page number)
 */

package com.propertycapsule.service.pdf;

public class Entry implements Comparable<Entry> {
    private Integer key;
    private String value;

    /**
     * 
     * @param pageNumber
     *            on which this property data was found
     * @param propertyData
     *            String representing an email, address, etc. from the PDF
     */
    public Entry(Integer pageNumber, String propertyData) {
        this.key = pageNumber;
        this.value = propertyData;
    }

    public Integer getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public int compareTo(Entry other) {
        return this.getKey().compareTo(other.getKey());
    }
}
