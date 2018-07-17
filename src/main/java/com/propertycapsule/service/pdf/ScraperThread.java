package com.propertycapsule.service.pdf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

public class ScraperThread implements Runnable {
    private Thread thread;
    private int threadPageNumber;
    private PDDocument document;

    public ScraperThread(PDDocument input, int pageNumber) {
        document = input;
        threadPageNumber = pageNumber;
    }

    public void run() {
        File inputTxt = AWS_Scrape.cleanTxt(pdfToTxt());
        scrapeTxt(inputTxt);
    }

    public void start() {
        if(thread == null) {
            thread = new Thread(this, Integer.toString(threadPageNumber));
            thread.start();
            try {
                thread.join();
            } catch(InterruptedException e) {
                System.out.println("Interrupted thread " + threadPageNumber);
                e.printStackTrace();
            }
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
                            Entry entry = new Entry(threadPageNumber, data);
                            ParallelScraper.address.offer(entry);
                        }
                    }
                } else if(line.matches(".*, [a-zA-Z]{2}( .*|[.])?")) {
                    // match ADDRESSES like "222 W Avenida Valencia,
                    // Orlando, FL" or "222 W Avenida Valencia, Round Rock,
                    // TX"
                    if(!(line.contains("suite") || line.contains("floor"))) {
                        if(line.length() < 70) {
                            Entry entry = new Entry(threadPageNumber, line);
                            ParallelScraper.address.offer(entry);
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
                            Entry entry = new Entry(threadPageNumber, "Lease");
                            ParallelScraper.term.offer(entry);
                        } else if(token.toLowerCase().equals("sale")) {
                            // match SALE TERMS of the property
                            Entry entry = new Entry(threadPageNumber, "Sale");
                            ParallelScraper.term.offer(entry);
                        } else if(token.matches("([a-zA-Z0-9]+[.])*[a-zA-Z0-9]+@[a-zA-Z0-9]+.*")) {
                            // match EMAILS
                            Entry entry = new Entry(threadPageNumber, token);
                            ParallelScraper.emails.offer(entry);
                        } else if(token.matches("s[.]?f[.]?") || token.matches("sq[.||ft]?") || token.equals("square")
                                || token.contains("ft.*") || token.equals("±")) {
                            // match SQUARE FOOTAGES like "4,500 sf" or
                            // "4,500 square feet"
                            if(lastToken.matches("[,±0-9/+//-]{3,}")) {
                                Entry entry = new Entry(threadPageNumber,
                                        lastToken.replace("±", "").replace("+/-", ""));
                                ParallelScraper.squareFootage.offer(entry);
                            }
                        } else if((token.matches("feet[:]?") || token.matches("(±||(/+//-))")) && ws.hasMoreTokens()) {
                            // match SQUARE FOOTAGES like "square feet:
                            // 4,500" or "± 4,500"
                            String next = ws.nextToken();
                            if(next.matches("[,±0-9/+//-]{3,}")) {
                                Entry entry = new Entry(threadPageNumber, next.replace("±", "").replace("+/-", ""));
                                ParallelScraper.squareFootage.offer(entry);
                            }
                        } else if(token.matches("[0-9]{3}") && lastToken.matches("[0-9]{3}") && ws.hasMoreTokens()) {
                            // match PHONE NUMBERS like "425 241 7707"
                            Entry entry = new Entry(threadPageNumber, lastToken + "-" + token + "-" + ws.nextToken());
                            ParallelScraper.phoneNumbers.offer(entry);
                        } else if(token.matches("([+]1[.])?[0-9]{3}[.][0-9]{3}[.][0-9]{4}.*")) {
                            // match PHONE NUMBERS like "425.241.7707"
                            Entry entry = new Entry(threadPageNumber, token.replace(".", "-"));
                            ParallelScraper.phoneNumbers.offer(entry);
                        } else if(token.matches("[0-9]{3}-[0-9]{3}-[0-9]{4}.*")) {
                            // match PHONE NUMBERS like "425-241-7707" or
                            // "425-341-7707,"
                            Entry entry = new Entry(threadPageNumber, token);
                            ParallelScraper.phoneNumbers.offer(entry);
                        } else if(token.matches("[(][0-9]{3}[)]") && ws.hasMoreTokens()) {
                            // match PHONE NUMBERS like "(425) 241-7707"
                            String theRest = ws.nextToken();
                            data = token.substring(1, 4) + "-" + theRest;
                            if(theRest.matches("[0-9]{3}-[0-9]{4}.*")) {
                                Entry entry = new Entry(threadPageNumber, data);
                                ParallelScraper.phoneNumbers.offer(entry);
                            }
                        }
                        lastToken = token;
                    } catch(java.lang.IndexOutOfBoundsException e) {
                        break;
                    }
                }
                previousLine = line;
            }
            // scrapeContactNames(emails, txtInput);
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
     * Scraper function for contact names
     * 
     * @param emails
     *            list of emails found in this pdf, will be parsed to find the
     *            name
     * @param txtInput
     *            txt file to scrape
     * @return arrayList of contact names for the pdf
     */
    public ArrayList<String> scrapeContactNames(ArrayList<String> emails, File txtInput) {
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
}
