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
import com.google.gson.*;
import com.google.maps.model.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.text.similarity.LevenshteinDistance;

public class Geocode {
    public static Keys properties = new Keys();
    public GeoApiContext context;
    public Gson gson;
    public HashMap<String, String> map = new HashMap<String, String>();
    public final String[] accurateTypes = { "STREET_NUMBER", "PREMISE", "SUBPREMISE", "INTERSECTION" };
    public final String[] approximateTypes = { "ROUTE", "LOCALITY", "POSTAL CODE", "NEIGHBORHOOD" };
    public LevenshteinDistance levDistance = new LevenshteinDistance();

    public Geocode() {
        context = new GeoApiContext.Builder().apiKey(properties.getKey("GoogleAPI")).build();
        gson = new GsonBuilder().setPrettyPrinting().create();
    }

    /**
     * @param scrapedAddress
     *            the address scraped from the pdf in Read_Text
     * @return Hashmap containing an address, latitude, and longitude supplied
     *         by Google Maps API
     */
    public void getParallelGeocodedInfo(String scrapedAddress) {
        try {
            GeocodingResult[] results = GeocodingApi.geocode(context, scrapedAddress).await();
            if(results.length > 0) {
                ParallelScrape.geocodedAddress = gson.toJson(results[0].formattedAddress).replaceAll("\"", "");
                ParallelScrape.addressType = gson.toJson(results[0].addressComponents[0].types[0]).replaceAll("\"", "");
                ParallelScrape.latitude = gson.toJson(results[0].geometry.location.lat);
                ParallelScrape.longitude = gson.toJson(results[0].geometry.location.lng);
                ParallelScrape.levDistance = getLevDistance(scrapedAddress, results[0].formattedAddress);
            }
        } catch(IOException e) {
            e.printStackTrace();
        } catch(InterruptedException i) {
            i.printStackTrace();
        } catch(ApiException a) {
            a.printStackTrace();
        }
    }
    
    /**
     * @param scrapedAddress
     *            the address scraped from the pdf in Read_Text
     * @return Hashmap containing an address, latitude, and longitude supplied
     *         by Google Maps API
     */
    public HashMap<String, String> getGeocodedInfo(String scrapedAddress) {
        // if map not cleared, failed geocode request returns last successful
        // address
        map.clear();
        try {
            GeocodingResult[] results = GeocodingApi.geocode(context, scrapedAddress).await();
            if(results.length > 0) {
                map.put("address", gson.toJson(results[0].formattedAddress));
                map.put("latitude", gson.toJson(results[0].geometry.location.lat));
                map.put("longitude", gson.toJson(results[0].geometry.location.lng));
                map.put("type", gson.toJson(results[0].addressComponents[0].types[0]).replaceAll("\"", ""));
                map.put("levDistance", getLevDistance(scrapedAddress, results[0].formattedAddress));
            }
        } catch(IOException e) {
            e.printStackTrace();
        } catch(InterruptedException i) {
            i.printStackTrace();
        } catch(ApiException a) {
            a.printStackTrace();
        }
        return map;
    }

    /**
     * Calculate the similarity of the scraped string and geocoded string using
     * LevDistance alg
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
