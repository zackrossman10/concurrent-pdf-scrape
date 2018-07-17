/**
 * @author zacharycolerossman
 * @version 7/2/18
 * 
 * Handle the creating of Google Geocoder context and input of 
 * scraped property addresses
 */

package com.propertycapsule.service.pdf;

import com.google.maps.*;
import com.google.maps.errors.ApiException;
import com.google.maps.model.*;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.text.similarity.LevenshteinDistance;

public class Geocode {
    public static Keys properties = new Keys();
    public GeoApiContext context;
    public final String[] accurateTypes = { "STREET_NUMBER", "PREMISE", "SUBPREMISE", "INTERSECTION" };
    public final String[] approximateTypes = { "ROUTE", "LOCALITY", "POSTAL CODE", "NEIGHBORHOOD" };
    public LevenshteinDistance levDistance = new LevenshteinDistance();

    public Geocode() {
        context = new GeoApiContext.Builder().apiKey(properties.getKey("GoogleAPI")).build();
    }

    /**
     * @param scrapedAddress
     *            the address scraped from the pdf in Read_Text
     * @return Hashmap containing an address, latitude, and longitude supplied
     *         by Google Maps API
     */
    public GeocodingResult[] getParallelGeocodedInfo(String scrapedAddress) {
        GeocodingResult[] results = new GeocodingResult[0];
        try {
            results = GeocodingApi.geocode(context, scrapedAddress).await();
        } catch(IOException e) {
            e.printStackTrace();
        } catch(InterruptedException i) {
            i.printStackTrace();
        } catch(ApiException a) {
            a.printStackTrace();
        }
        return results;
    }

    /**
     * Calculate the similarity of the scraped string and geocoded string using
     * LevDistance alg (NOT USED IN VERION 7/18/18)
     * 
     * @param scrapedAddress
     * @param geocodedAddress
     * @return the calculated lev distance as a string
     */
    public String getLevDistance(String scrapedAddress, String geocodedAddress) {
        // take substrings of scraped/geocoded addressed up to state
        // abbreviations, results in more comparable substrings
        Pattern scrapePattern = Pattern.compile(".*, [a-zA-Z]{2}( |[.])?");
        Pattern geoPattern = Pattern.compile(", [A-Z]{2} ");
        Matcher scrapeMatcher = scrapePattern.matcher(scrapedAddress);
        Matcher geoMatcher = geoPattern.matcher(geocodedAddress);
        int scrapedEndIndex = scrapeMatcher.find() ? scrapeMatcher.end() : scrapedAddress.length();
        int geoIndex = geoMatcher.find() ? geoMatcher.end() : geocodedAddress.length();
        // find lev distance between the two modified address strings
        int levD = levDistance.apply(scrapedAddress.substring(0, scrapedEndIndex),
                geocodedAddress.substring(0, geoIndex).toLowerCase());
        return Integer.toString(levD);
    }

}
