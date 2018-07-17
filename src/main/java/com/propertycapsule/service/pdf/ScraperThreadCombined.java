package com.propertycapsule.service.pdf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

public class ScraperThreadCombined implements Runnable {
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
    public Geocode geocoder = new Geocode();
    public PriorityQueue<Entry> address = new PriorityQueue<Entry>();
    public PriorityQueue<Entry> term = new PriorityQueue<Entry>();
    public PriorityQueue<Entry> squareFootage = new PriorityQueue<Entry>();
    public PriorityQueue<Entry> emails = new PriorityQueue<Entry>();
    public PriorityQueue<Entry> phoneNumbers = new PriorityQueue<Entry>();
    public PriorityQueue<Entry> contactNames = new PriorityQueue<Entry>();
    public String geocodedAddress = "";
    public String addressType = "";
    public String latitude = "";
    public String longitude = "";
    public String levDistance = "";
    private Thread thread;
    private int threadPageNumber;
    private PDDocument document;

    public ScraperThreadCombined(PDDocument input, int pageNumber) {
        document = input;
        threadPageNumber = pageNumber;
    }

    public File scrape(File input) {
        // File input = new
        // File("/Users/zacharycolerossman/Documents/ML_Flyer_Data/Complete_Test_Set/144
        // Parnassas_colliers.pdf");
        PDDocument document = null;
        try {
            document = PDDocument.load(input);
            int numPages = document.getNumberOfPages();
            for(int i = 1; i <= numPages; i++) {
                start();
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
        return resultsToJson();
    }

    public void getGeocodedAddress() {
        if(!address.isEmpty()) {
            // normalize address strings for better consistency with
            // geocoder
            String cleanEntry = address.peek().getValue().replace(" - ", "-").replace(" – ", "-").replace("–", "-")
                    .replace("street", "st").toLowerCase();
            // translate addresses like "919-920 bath st." to "919 bath st."
            // for closer geocoder comparison
            if(cleanEntry.matches("[0-9]*-[0-9]* .*")) {
                cleanEntry = cleanEntry.substring(0, cleanEntry.indexOf("-"))
                        + cleanEntry.substring(cleanEntry.indexOf(" "));
            }
            geocoder.getParallelGeocodedInfo(cleanEntry, this);
        }
    }

    public void run() {
        File inputTxt = cleanTxt(pdfToTxt());
        scrapeTxt(inputTxt);
    }

    public void start(int pageNumber) {
        if(thread == null) {
            thread = new Thread(this, Integer.toString(threadPageNumber));
            thread.start();
        }
    }

    /**
     * Scrape a txt file for property criteria
     * 
     * @param filename
     *            name of the txt file to scrape
     * @return a hashmap containing categorized property data for this file
     */
    public void scrapeTxt(File txtInput) {
        BufferedReader bufferReader = null;
        // wordstream supplies tokens from each line
        WordStream ws = new WordStream();
        try {
            bufferReader = new BufferedReader(new FileReader(txtInput));
            String line = "";
            String previousLine = "";
            String data = "";
            while((line = bufferReader.readLine()) != null) {
                // replace all horizontal whitespace chars with spaces for regex
                // matching
                line = line.replaceAll(" ", " ");
                String lastToken = "";
                ws.addLexItems(line);
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
                            data = previousLine.length() < 35 ? (previousLine + ", " + line) : line;
                            address.offer(new Entry(threadPageNumber, data));
                        }
                    }
                } else if(line.matches(".*, [a-zA-Z]{2}( .*|[.])?")) {
                    // match ADDRESSES like "222 W Avenida Valencia,
                    // Orlando, FL" or "222 W Avenida Valencia, Round Rock,
                    // TX"
                    if(!(line.contains("suite") || line.contains("floor"))) {
                        if(line.length() < 70) {
                            address.offer(new Entry(threadPageNumber, line));
                        }
                    }
                }
                while(ws.hasMoreTokens()) {
                    // surround with try/catch to protect against weird
                    // character replacement/parsing issue
                    try {
                        String token = ws.nextToken();
                        if(token.toLowerCase().equals("lease") || token.toLowerCase().equals("leased")) {
                            // match LEASE TERMS of the property
                            term.offer(new Entry(threadPageNumber, "Lease"));
                        } else if(token.toLowerCase().equals("sale")) {
                            // match SALE TERMS of the property
                            term.offer(new Entry(threadPageNumber, "Sale"));
                        } else if(token.matches("([a-zA-Z0-9]+[.])*[a-zA-Z0-9]+@[a-zA-Z0-9]+.*")) {
                            // match EMAILS
                            emails.offer(new Entry(threadPageNumber, token));
                        } else if(token.matches("s[.]?f[.]?") || token.matches("sq[.||ft]?") || token.equals("square")
                                || token.contains("ft.*") || token.equals("±")) {
                            // match SQUARE FOOTAGES like "4,500 sf" or
                            // "4,500 square feet"
                            if(lastToken.matches("[,±0-9/+//-]{3,}")) {
                                squareFootage.offer(
                                        new Entry(threadPageNumber, lastToken.replace("±", "").replace("+/-", "")));
                            }
                        } else if((token.matches("feet[:]?") || token.matches("(±||(/+//-))")) && ws.hasMoreTokens()) {
                            // match SQUARE FOOTAGES like "square feet:
                            // 4,500" or "± 4,500"
                            String next = ws.nextToken();
                            if(next.matches("[,±0-9/+//-]{3,}")) {
                                squareFootage
                                        .offer(new Entry(threadPageNumber, next.replace("±", "").replace("+/-", "")));
                            }
                        } else if(token.matches("[0-9]{3}") && lastToken.matches("[0-9]{3}") && ws.hasMoreTokens()) {
                            // match PHONE NUMBERS like "425 241 7707"
                            Entry entry = new Entry(threadPageNumber, lastToken + "-" + token + "-" + ws.nextToken());
                            phoneNumbers.offer(entry);
                        } else if(token.matches("([+]1[.])?[0-9]{3}[.][0-9]{3}[.][0-9]{4}.*")) {
                            // match PHONE NUMBERS like "425.241.7707"
                            phoneNumbers.offer(new Entry(threadPageNumber, token.replace(".", "-")));
                        } else if(token.matches("[0-9]{3}-[0-9]{3}-[0-9]{4}.*")) {
                            // match PHONE NUMBERS like "425-241-7707" or
                            // "425-341-7707,"
                            phoneNumbers.offer(new Entry(threadPageNumber, token));
                        } else if(token.matches("[(][0-9]{3}[)]") && ws.hasMoreTokens()) {
                            // match PHONE NUMBERS like "(425) 241-7707"
                            String theRest = ws.nextToken();
                            data = token.substring(1, 4) + "-" + theRest;
                            if(theRest.matches("[0-9]{3}-[0-9]{4}.*")) {
                                phoneNumbers.offer(new Entry(threadPageNumber, data));
                            }
                        }
                        lastToken = token;
                    } catch(java.lang.IndexOutOfBoundsException e) {
                        break;
                    }
                }
                previousLine = line;
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
    }

    /**
     * Use PDFBox to extract text from a PDF, write to a txt file
     * 
     * @param input
     *            PDF file to scrape
     * @return txt file which contains PDF text
     */
    public File pdfToTxt() {
        File outputTxt = AWS_Wrapper.createTmp("output", ".txt");
        PDFTextStripper pdfStripper = null;
        FileWriter writer = null;
        try {
            pdfStripper = new PDFTextStripper();
            pdfStripper.setStartPage(threadPageNumber);
            pdfStripper.setEndPage(threadPageNumber);
            String pdfText = pdfStripper.getText(document);
            // pdf text parses better when spaces replaced with special char
            // "~", will be
            pdfText = pdfText.replace(" ", "~");
            writer = new FileWriter(outputTxt);
            writer.write(pdfText);
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
}
