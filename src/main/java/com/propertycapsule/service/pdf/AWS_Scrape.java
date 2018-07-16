/**
 * @author zacharycolerossman
 * @version 7/2/18
= * 
 * 1) translate multiple PDFs into .txt files (using PDFBox)
 * 2) scrape desired information related to each property
 * 3) export that scraped information as a .csv or .json file
 */

package com.propertycapsule.service.pdf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class AWS_Scrape {
    public static final HashMap<String, String> abbreviations = new HashMap<String, String>() {
        {
            put("alabama", "AL");
            put("alaska", "AK");
            put("arizona", "AZ");
            put("arkansas", "AR");
            put("california", "CA");
            put("colorado", "CO");
            put("connecticut", "CT");
            put("delaware", "DE");
            put("florida", "FL");
            put("georgia", "GA");
            put("hawaii", "HI");
            put("idaho", "ID");
            put("illinois", "IL");
            put("indiana", "IN");
            put("iowa", "IA");
            put("kansas", "KS");
            put("kentucky", "KY");
            put("lousisiana", "LA");
            put("maine", "ME");
            put("maryland", "MD");
            put("massachusetts", "MA");
            put("michigan", "MI");
            put("minnesota", "MN");
            put("mississippi", "MS");
            put("missouri", "MO");
            put("montana", "MT");
            put("nebraska", "NE");
            put("nevada", "NV");
            put("new hampshire", "NH");
            put("new jersey", "NJ");
            put("new mexico", "NM");
            put("new york", "NY");
            put("north carolina", "NC");
            put("north dakota", "ND");
            put("ohio", "OH");
            put("oklahoma", "OK");
            put("oregon", "OR");
            put("pennsylvania", "PA");
            put("rhode island", "RI");
            put("south carolina", "SC");
            put("south dakota", "SD");
            put("tennessee", "TN");
            put("texas", "TX");
            put("utah", "UT");
            put("vermont", "VT");
            put("virginia", "VA");
            put("d.c.", "DC");
            put("washington", "WA");
            put("west virginia", "WV");
            put("wisconson", "WI");
            put("wyoming", "WY");
        }
    };
    public static Geocode geocoder = new Geocode();
    public static final String outputFilePath = "/Users/zacharycolerossman/Documents/ML_Flyer_Data/outputTxts/";

    /**
     * Main wrapper function for scraping a pdf file
     * 
     * @param input
     *            PDF file to scrape
     * @return a .json file containing the results
     */
    public static File scrape(File input) {
        File messyTxt = pdfToTxt(input);
        HashMap<String, ArrayList<String>> data = scrapeTxt(cleanTxt(messyTxt));
        return resultsToJson(data);
    }

    /**
     * Use PDFBox to extract text from a PDF, write to a txt file
     * 
     * @param input
     *            PDF file to scrape
     * @return txt file which contains PDF text
     */
    public static File pdfToTxt(File input) {
        File outputTxt = AWS_Wrapper.createTmp("output", ".txt");
        PDFTextStripper pdfStripper = null;
        PDDocument document = null;
        FileWriter writer = null;
        try {
            pdfStripper = new PDFTextStripper();
            document = PDDocument.load(input);
            writer = new FileWriter(outputTxt);
            String pdfText = pdfStripper.getText(document);
            // pdf text parses better when spaces replaced with special char
            // "~", will be
            pdfText = pdfText.replace(" ", "~");
            writer.write(pdfText);
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
            if(writer != null) {
                try {
                    writer.close();
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return outputTxt;
    }

    /**
     * Reformat a txt file, prepare for parsing
     * 
     * @param input
     *            txt file to clean up
     * @return clean txt file ready for parsing
     */
    public static File cleanTxt(File messyTxt) {
        FileWriter writer2 = null;
        BufferedReader bufferReader = null;
        try {
            bufferReader = new BufferedReader(new FileReader(messyTxt));
            String oldContent = "";
            String line = bufferReader.readLine();
            while(line != null) {
                line = line.toLowerCase();
                // edge case for unnecessary spaces between text (e.g. "4 2 A V
                // E N U E")
                if(line.matches("[~]?[^~][~][^~][~][~]?([^~]?[^~][~][~]?)*[^~]?")) {
                    line = line.replaceAll("~~", " ").replaceAll("~", "");
                }
                // edge case for addresses ending in "... San Francisco" and
                // missing state/zip
                if(line.matches(".*san~francisco")) {
                    line += ", ca";
                }
                oldContent = oldContent + line + System.lineSeparator();
                line = bufferReader.readLine();
            }
            // reformat special chars for better parsing
            String newContent = oldContent.replaceAll("~~", " ").replaceAll("~", " ").replace(" •", ",").replace(" |",
                    ",");
            for(HashMap.Entry<String, String> entry : abbreviations.entrySet()) {
                // replace full state names with abbreviations for regex
                // matching (e.g. "florida" -> "FL")
                newContent = newContent.replaceAll(entry.getKey(), entry.getValue());
            }
            writer2 = new FileWriter(messyTxt);
            writer2.write(newContent + " ");
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            if(bufferReader != null) {
                try {
                    bufferReader.close();
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }
            if(writer2 != null) {
                try {
                    writer2.close();
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return messyTxt;
    }

    /**
     * Scrape a txt file for property criteria
     * 
     * @param filename
     *            name of the txt file to scrape
     * @return a hashmap containing categorized property data for this file
     */
    public static HashMap<String, ArrayList<String>> scrapeTxt(File txtInput) {
        // hashmap holding arrays of different information for this pdf
        HashMap<String, ArrayList<String>> propertyData = new HashMap<String, ArrayList<String>>();
        // arrays which accumulate data by category
        ArrayList<String> addresses = new ArrayList<String>();
        ArrayList<String> geocodedAddress = new ArrayList<String>();
        ArrayList<String> latitude = new ArrayList<String>();
        ArrayList<String> longitude = new ArrayList<String>();
        ArrayList<String> addressType = new ArrayList<String>();
        ArrayList<String> levDistance = new ArrayList<String>();
        ArrayList<String> terms = new ArrayList<String>();
        ArrayList<String> emails = new ArrayList<String>();
        ArrayList<String> squareFootages = new ArrayList<String>();
        ArrayList<String> phoneNums = new ArrayList<String>();
        BufferedReader bufferReader = null;
        // wordstream supplies tokens from each line
        WordStream ws = new WordStream();
        try {
            bufferReader = new BufferedReader(new FileReader(txtInput));
            String line = "";
            String previousLine = "";
            String entry = "";
            while((line = bufferReader.readLine()) != null) {
                // replace all horizontal whitespace chars with spaces for regex
                // matching
                line = line.replaceAll(" ", " ");
                String lastToken = "";
                ws.addLexItems(line);
                if(addresses.isEmpty()) {
                    if(line.matches("[a-zA-Z'@ ]*, [a-zA-Z]{2}( .*|[.])?")) {
                        // match ADDRESSES like "Orlando, FL" or "Round Rock, ca
                        // 91711" (with street address on the previous line)
                        if(!(previousLine.contains("suite") || previousLine.contains("floor") || line.contains("suite")
                                || line.contains("floor"))) {
                            // avoid address with "suite" or "floor", usaully
                            // the office address
                            if(line.length() < 35) {
                                // avoid matching random text by limiting
                                // linelength
                                entry = previousLine.length() < 35 ? (previousLine + ", " + line) : line;
                                addresses.add(entry);
                            }
                        }
                    } else if(line.matches(".*, [a-zA-Z]{2}( .*|[.])?")) {
                        // match ADDRESSES like "222 W Avenida Valencia,
                        // Orlando, FL" or "222 W Avenida Valencia, Round Rock,
                        // TX"
                        if(!(line.contains("suite") || line.contains("floor"))) {
                            if(line.length() < 70) {
                                addresses.add(line);
                            }
                        }
                    }
                }
                while(ws.hasMoreTokens()) {
                    // surround with try/catch to protect against weird
                    // character replacement/parsing issue
                    try {
                        String token = ws.nextToken();
                        if(terms.isEmpty()
                                && (token.toLowerCase().equals("lease") || token.toLowerCase().equals("leased"))) {
                            // match LEASE TERMS of the property
                            terms.add("Lease");
                        } else if(terms.isEmpty() && token.toLowerCase().equals("sale")) {
                            // match SALE TERMS of the property
                            terms.add("Sale");
                        } else if(token.matches("([a-zA-Z0-9]+[.])*[a-zA-Z0-9]+@[a-zA-Z0-9]+.*")
                                && !emails.contains(token)) {
                            // match EMAILS
                            emails.add(token);
                        } else if(squareFootages.isEmpty()) {
                            token = token.toLowerCase();
                            if(token.matches("s[.]?f[.]?") || token.matches("sq[.||ft]?") || token.equals("square")
                                    || token.contains("ft.*") || token.equals("±")) {
                                // match SQUARE FOOTAGES like "4,500 sf" or
                                // "4,500 square feet"
                                if(lastToken.matches("[,±0-9/+//-]{3,}")) {
                                    squareFootages.add(lastToken.replace("±", "").replace("+/-", ""));
                                }
                            } else if((token.matches("feet[:]?") || token.matches("(±||(/+//-))"))
                                    && ws.hasMoreTokens()) {
                                // match SQUARE FOOTAGES like "square feet:
                                // 4,500" or "± 4,500"
                                String next = ws.nextToken();
                                if(next.matches("[,±0-9/+//-]{3,}")) {
                                    squareFootages.add(next.replace("±", "").replace("+/-", ""));
                                }
                            }
                        } else if(token.matches("[0-9]{3}") && lastToken.matches("[0-9]{3}") && ws.hasMoreTokens()) {
                            // match PHONE NUMBERS like "425 241 7707"
                            entry = lastToken + "-" + token + "-" + ws.nextToken();
                            if(!phoneNums.contains(entry)) {
                                phoneNums.add(entry);
                            }
                        } else if(token.matches("([+]1[.])?[0-9]{3}[.][0-9]{3}[.][0-9]{4}.*")) {
                            // match PHONE NUMBERS like "425.241.7707"
                            entry = token.replace(".", "-");
                            if(!phoneNums.contains(entry)) {
                                phoneNums.add(entry);

                            }
                        } else if(token.matches("[0-9]{3}-[0-9]{3}-[0-9]{4}.*") && !phoneNums.contains(token)) {
                            // match PHONE NUMBERS like "425-241-7707" or
                            // "425-341-7707,"
                            phoneNums.add(token);
                        } else if(token.matches("[(][0-9]{3}[)]") && ws.hasMoreTokens()) {
                            // match PHONE NUMBERS like "(425) 241-7707"
                            String theRest = ws.nextToken();
                            entry = token.substring(1, 4) + "-" + theRest;
                            if(theRest.matches("[0-9]{3}-[0-9]{4}.*") && !phoneNums.contains(entry)) {
                                phoneNums.add(entry);
                            }
                        }
                        lastToken = token;
                    } catch(java.lang.IndexOutOfBoundsException e) {
                        break;
                    }
                }
                previousLine = line;
            }
            if(!addresses.isEmpty()) {
                // normalize address strings to for better consistency with
                // geocoder
                String cleanEntry = addresses.get(0).replace(" - ", "-").replace(" – ", "-").replace("–", "-")
                        .replace("street", "st").toLowerCase();
                // translate addresses like "919-920 bath st." to "919 bath st."
                // for better geocoder matching
                if(cleanEntry.matches("[0-9]*-[0-9]* .*")) {
                    cleanEntry = cleanEntry.substring(0, cleanEntry.indexOf("-"))
                            + cleanEntry.substring(cleanEntry.indexOf(" "));
                }
                HashMap<String, String> geocodedInfo = geocoder.getGeocodedInfo(cleanEntry);
                if(geocodedInfo.get("address") != null) {
                    geocodedAddress.add(geocodedInfo.get("address").replace("\\u0026", "&").replace("\\u0027", "'")
                            .replaceAll("\"", ""));
                    latitude.add(geocodedInfo.get("latitude"));
                    longitude.add(geocodedInfo.get("longitude"));
                    addressType.add(geocodedInfo.get("type"));
                    levDistance.add(geocodedInfo.get("levDistance"));
                } else {
                    addresses.remove(0);
                    addresses.add("**INACCURATE**");
                    geocodedAddress.add("*INACCURATE**");
                    latitude.add("**INACCURATE**");
                    longitude.add("**INACCURATE**");
                }
            } else {
                addresses.add("**Unknown**");
                geocodedAddress.add("**Unknown**");
                latitude.add("**Unknown**");
                longitude.add("**Unknown**");
            }
            if(terms.isEmpty()) {
                terms.add("**Unknown**");
            }
            if(squareFootages.isEmpty()) {
                squareFootages.add("**Unknown**");
            }
            if(emails.isEmpty()) {
                emails.add("**Unknown**");
            }
            if(phoneNums.isEmpty()) {
                phoneNums.add("**Unknown**");
            }
            propertyData.put("Address", addresses);
            propertyData.put("Geocoded Address", geocodedAddress);
            propertyData.put("Latitude", latitude);
            propertyData.put("Longitude", longitude);
            propertyData.put("levDistance", levDistance);
            propertyData.put("Type", addressType);
            propertyData.put("Term", terms);
            propertyData.put("Square Footage", squareFootages);
            propertyData.put("Emails", emails);
            propertyData.put("Phone Numbers", phoneNums);
            ArrayList<String> contacts = scrapeContactNames(emails, txtInput);
            propertyData.put("Contact Names", contacts);
        } catch(FileNotFoundException ex) {
            ex.printStackTrace();
        } catch(IOException ex) {
            ex.printStackTrace();
        } finally {
            if(bufferReader != null) {
                try {
                    bufferReader.close();
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return propertyData;
    }

    /**
     * Scraper function for contact names
     * 
     * @param emails
     *            list of emails found in this pdf, will be parsed to find the
     *            name
     * @param txtInput
     *            txt file to scrape
     * @return arrayList of contact names for the pdf
     */
    public static ArrayList<String> scrapeContactNames(ArrayList<String> emails, File txtInput) {
        ArrayList<String> contacts = new ArrayList<String>();
        WordStream ws = new WordStream();
        BufferedReader bufferReader = null;
        try {
            bufferReader = new BufferedReader(new FileReader(txtInput));
            String line, entry, token, lastToken;
            line = entry = token = lastToken = "";
            boolean hasPeriod, foundContact;
            hasPeriod = foundContact = false;
            if(emails.get(0).equals("**Unknown**")) {
                contacts.add("**Unknown**");
            } else {
                for(String email : emails) {
                    // isolate name from email address ("zack@..."
                    // -> "zack")
                    String searchName = email.substring(0, email.indexOf("@")).toLowerCase();
                    if(searchName.contains(".")) {
                        // search for "zack" if email is "zack.rossman@..."
                        searchName = searchName.substring(0, searchName.indexOf("."));
                        hasPeriod = true;
                    }
                    while(!foundContact && (line = bufferReader.readLine()) != null) {
                        ws.addLexItems(line);
                        while(ws.hasMoreTokens()) {
                            try {
                                token = ws.nextToken().toLowerCase();
                                if(token.contains(searchName) && ws.hasMoreTokens()) {
                                    String lastName = ws.nextToken();
                                    // format the name
                                    entry = token.substring(0, 1).toUpperCase() + token.substring(1).toLowerCase() + " "
                                            + lastName.substring(0, 1).toUpperCase()
                                            + lastName.substring(1).toLowerCase();
                                } else if(!hasPeriod && token.contains(searchName.substring(1, searchName.length()))) {
                                    // search for last name for emails with
                                    // format zrossman@...
                                    entry = lastToken.substring(0, 1).toUpperCase()
                                            + lastToken.substring(1).toLowerCase() + " "
                                            + token.substring(0, 1).toUpperCase() + token.substring(1).toLowerCase();
                                }
                                if(entry.contains("@")) {
                                    // for case that email itself is the
                                    // only matched string
                                    entry = entry.substring(entry.indexOf(0), entry.indexOf("@"));
                                }
                                if(entry.length() > 0) {
                                    contacts.add(entry);
                                    foundContact = true;
                                }
                                entry = "";
                                lastToken = token;
                            } catch(java.lang.IndexOutOfBoundsException e) {
                                break;
                            }
                        }
                    }
                    if(!foundContact) {
                        contacts.add("**Not found** " + searchName);
                    }
                    lastToken = "";
                    foundContact = false;
                }
            }
        } catch(FileNotFoundException ex) {
            ex.printStackTrace();
        } catch(IOException ex) {
            ex.printStackTrace();
        } finally {
            if(bufferReader != null) {
                try {
                    bufferReader.close();
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return contacts;
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
    public static File resultsToJson(HashMap<String, ArrayList<String>> results) {
        File jsonOutput = AWS_Wrapper.createTmp("output", ".json");
        try {
            jsonOutput.createNewFile();
        } catch(IOException e1) {
            e1.printStackTrace();
        }
        JSONObject fileObject = new JSONObject();
        for(HashMap.Entry<String, ArrayList<String>> entry : results.entrySet()) {
            String criteria = entry.getKey();
            if(criteria.equals("Emails") || criteria.equals("Phone Numbers") || criteria.equals("Contact Names")) {
                JSONArray list = new JSONArray();
                for(String result : entry.getValue()) {
                    list.add(result);
                }
                fileObject.put(criteria, list);
            } else {
                if(entry.getValue().size() > 0) {
                    fileObject.put(criteria, entry.getValue().get(0));
                }
            }
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
