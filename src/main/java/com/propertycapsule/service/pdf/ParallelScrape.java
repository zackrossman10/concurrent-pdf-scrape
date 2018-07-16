/***
 * @author zacharycolerossman 
 * @version 7/17/18
 * 
 * Class which facilitates the scraping of PDF data using threads
 * 
 */

package com.propertycapsule.service.pdf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class ParallelScrape {
    public static HashMap<String, String> abbreviations = AWS_Scrape.abbreviations;
    public static Geocode geocoder = new Geocode();
    public static PriorityQueue<Entry> address = new PriorityQueue<>();
    public static PriorityQueue<Entry> term = new PriorityQueue<>();
    public static PriorityQueue<Entry> squareFootage = new PriorityQueue<>();
    public static PriorityQueue<Entry> emails = new PriorityQueue<>();
    public static PriorityQueue<Entry> phoneNumbers = new PriorityQueue<>();
    public static PriorityQueue<Entry> contactNames = new PriorityQueue<>();
    public static String geocodedAddress = "";
    public static String addressType = "";
    public static String latitude = "";
    public static String longitude = "";
    public static String levDistance = "";

     
    public static void main(String[] args) {
        File input = new File("/Users/zacharycolerossman/Documents/ML_Flyer_Data/Complete_Test_Set/144 Parnassas_colliers.pdf");
        PDDocument document = null;
        try {
            document = PDDocument.load(input);
            int numPages = document.getNumberOfPages();
            for(int i = 1; i<=numPages; i++) {
                ScraperThread thread = new ScraperThread(document, i);
                thread.start();
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            if(document != null) {
                try {
                    document.close();
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }
        getGeocodedAddress();
        resultsToJson();
    }
    
    public static void getGeocodedAddress() {
        if(!address.isEmpty()) {
            // normalize address strings to for better consistency with
            // geocoder
            String cleanEntry = address.peek().getValue().replace(" - ", "-").replace(" – ", "-").replace("–", "-")
                    .replace("street", "st").toLowerCase();
            // translate addresses like "919-920 bath st." to "919 bath st."
            // for better geocoder matching
            if(cleanEntry.matches("[0-9]*-[0-9]* .*")) {
                cleanEntry = cleanEntry.substring(0, cleanEntry.indexOf("-"))
                        + cleanEntry.substring(cleanEntry.indexOf(" "));
            }
            geocoder.getParallelGeocodedInfo(cleanEntry);
        }
    }
    
    /**
     * Format the scraped information from all PDFs into one new .json file
     * 
     * @param results
     *            contains the property data for every pdf file processed
     * @param inputPdfNames
     *            -> arraylist of names of pdfs that were scraped
     * @return json file containing property information
     */
    public static File resultsToJson() {
        File jsonOutput = new File("/Users/zacharycolerossman/Documents/ML_Flyer_Data/Output_Txts/HIIII.json");
        try {
            jsonOutput.createNewFile();
        } catch(IOException e1) {
            e1.printStackTrace();
        }
        JSONObject fileObject = new JSONObject();
        if(!address.isEmpty()) {
            fileObject.put("address", address.peek().getValue());
            fileObject.put("geocoded_address", geocodedAddress);
            fileObject.put("address_type", addressType);
            fileObject.put("latitude", latitude);
            fileObject.put("longitude", longitude);
            fileObject.put("lev_distance", levDistance);
        }
        if(!term.isEmpty()) {
            fileObject.put("Term", term.peek().getValue());
        }
        if(!squareFootage.isEmpty()) {
            fileObject.put("Square Footage", squareFootage.peek().getValue());
        }
        if(!emails.isEmpty()) {
            JSONArray list = new JSONArray();
            while(!emails.isEmpty()) {
                String data = emails.poll().getValue();
                if(!list.contains(data)) {
                    list.add(data);
                }
            }
            fileObject.put("Emails", list);
        }
        if(!phoneNumbers.isEmpty()) {
            JSONArray list = new JSONArray();
            while(!phoneNumbers.isEmpty()) {
                String data = phoneNumbers.poll().getValue();
                if(!list.contains(data)) {
                    list.add(data);
                }
            }
            fileObject.put("Phone Numbers", list);
        }
        if(!contactNames.isEmpty()) {
            JSONArray list = new JSONArray();
            while(!contactNames.isEmpty()) {
                String data = contactNames.poll().getValue();
                if(!list.contains(data)) {
                    list.add(data);
                }
            }
            fileObject.put("Contact Names", list);
        }
        FileWriter filew = null;
        try {
            filew = new FileWriter(jsonOutput);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            JsonParser jp = new JsonParser();
            JsonElement je = jp.parse(fileObject.toJSONString());
            filew.write(gson.toJson(je));
            filew.flush();
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            if(filew != null) {
                try {
                    filew.close();
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return jsonOutput;
    }
}
