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
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class ParallelScraper {
    //priority queues to accumulate property data scraped by threads operating in parallel
    public static PriorityQueue<Entry> address;
    public static PriorityQueue<Entry> term;
    public static PriorityQueue<Entry> squareFootage;
    public static PriorityQueue<Entry> emails;
    public static PriorityQueue<Entry> phoneNumbers;
    public static PriorityQueue<Entry> contactNames;
    //variables modified by Geocoder
    public static String geocodedAddress;
    public static String addressType;
    public static String latitude;
    public static String longitude;
    public static String levDistance;

    public static Geocode geocoder;
    public static final int parsedPageLength = 2000;
    public static final int poolSize = 5;

    public ParallelScraper() {
        //redeclare static vars to start from clean slate
        geocoder = new Geocode();
        address = new PriorityQueue<>();
        term = new PriorityQueue<>();
        squareFootage = new PriorityQueue<>();
        emails = new PriorityQueue<>();
        phoneNumbers = new PriorityQueue<>();
        contactNames = new PriorityQueue<>();
        geocodedAddress = "";
        addressType = "";
        latitude = "";
        longitude = "";
        levDistance = "";
    }

    public File scrape(File input) {
        PDDocument document = null;
        try {
            document = PDDocument.load(input);
            //translate PDF into multiple txt files of size <parsedPageLength> chars
            ArrayList<File> txtFiles = stringToMultipleTxts(pdfToString(document));
            //create pool for threading
            ExecutorService pool = Executors.newFixedThreadPool(poolSize);
            List<Runnable> arrTasks = new ArrayList<Runnable>();
            int pageCounter = 1;
            //accumulate tasks
            for(File file : txtFiles) {
                Runnable task = new ScrapeTask(file, pageCounter);
                arrTasks.add(task);
                pageCounter++;
            }
            for(Runnable task : arrTasks) {
                pool.execute(task);
            }
            pool.shutdown();
            //wait for threads to finish
            try {
                while(!pool.awaitTermination(24L, TimeUnit.HOURS)) {
                    System.out.println("Not yet. Still waiting for termination");
                }
            } catch(InterruptedException e) {
                e.printStackTrace();
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
        //Use Geocoder API to validate address, get property lat/long
        getGeocodedAddress();  
        return resultsToJson();
    }

    public void getGeocodedAddress() {
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
     * Use PDFBox to extract text from a PDF to a string
     * 
     * @param input
     *            PDF file to scrape
     * @return txt file which contains PDF text
     */
    public String pdfToString(PDDocument document) {
        PDFTextStripper pdfStripper = null;
        String pdfString = "";
        try {
            pdfStripper = new PDFTextStripper();
            pdfString = pdfStripper.getText(document);
            // pdf text parses better when spaces replaced with special char
            // "~", will be
            pdfString = pdfString.replace(" ", "~");
        } catch(IOException e) {
            e.printStackTrace();
        }
        return pdfString;
    }

    /**
     * Write a string with PDF text to multiple smaller txt files
     * 
     * @param pdfText
     *            the string containing PDF text from PDFStripper
     * @return ArrayList of txt files to be scraped
     */
    public ArrayList<File> stringToMultipleTxts(String pdfText) {
        ArrayList<File> outputTxts = new ArrayList<File>();
        int textLength = pdfText.length();
        int startIndex = 0;
        //use parsedPageLength to determine character size of each file
        int endIndex = textLength > parsedPageLength ? parsedPageLength : textLength;
        String textSegment = "";
        File outputTxt = null;
        FileWriter writer = null;
        int pageCounter = 1;
        while(true) {
            textSegment = pdfText.substring(startIndex, endIndex);
            try {
                outputTxt = AWS_Wrapper.createTmp("Output" + Integer.toString(pageCounter), ".txt");
                writer = new FileWriter(outputTxt);
                writer.write(textSegment);
            } catch(IOException e) {
                e.printStackTrace();
            } finally {
                if(writer != null) {
                    try {
                        writer.close();
                    } catch(IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            outputTxts.add(outputTxt);
            if(endIndex == textLength)
                break;
            startIndex += parsedPageLength;
            endIndex = textLength > (startIndex + parsedPageLength) ? (startIndex + parsedPageLength) : textLength;
        }
        return outputTxts;
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
    public File resultsToJson() {
        File jsonOutput = AWS_Wrapper.createTmp("output", ".json");
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
            fileObject.put("term", term.peek().getValue());
        }
        if(!squareFootage.isEmpty()) {
            fileObject.put("square_footage", squareFootage.peek().getValue());
        }
        if(!emails.isEmpty()) {
            JSONArray list = new JSONArray();
            while(!emails.isEmpty()) {
                String data = emails.poll().getValue();
                if(!list.contains(data)) {
                    list.add(data);
                }
            }
            fileObject.put("emails", list);
        }
        if(!phoneNumbers.isEmpty()) {
            JSONArray list = new JSONArray();
            while(!phoneNumbers.isEmpty()) {
                String data = phoneNumbers.poll().getValue();
                if(!list.contains(data)) {
                    list.add(data);
                }
            }
            fileObject.put("phone_numbers", list);
        }
        if(!contactNames.isEmpty()) {
            JSONArray list = new JSONArray();
            while(!contactNames.isEmpty()) {
                String data = contactNames.poll().getValue();
                if(!list.contains(data)) {
                    list.add(data);
                }
            }
            fileObject.put("contact_names", list);
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
