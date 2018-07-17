/**
 * @author Zack Rossman
 * @version 7/9/18
 * 
 * Class used to test the scraping of PDFs in AWS_Scrape class
 */

package com.propertycapsule.service.pdf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;

public class TestOutput {
    public static final String s3OutputBucket = AWS_Wrapper.s3OutputBucket;
    public static final AmazonS3 s3Client = AWS_Wrapper.s3Client;
    public static final String outputFilePath = "/Users/zacharycolerossman/Documents/ML_Flyer_Data/Output_Txts/";
    public static String inputPdfName = "";

    public static void main(String[] args) {
        scrapeDirectory("/Users/zacharycolerossman/Documents/ML_Flyer_Data/Complete_Test_Set/");
    }

    /**
     * Get the list of all files in the "input folder
     * 
     * @param folder
     *            the folder containing pdf files to be scraped
     * @return arraylist of file names for PDFs to be translated to txt and
     *         scraped
     */
    public static ArrayList<String> getInputFilenames(final File folder) {
        ArrayList<String> inputFileNames = new ArrayList<String>();
        for(final File fileEntry : folder.listFiles()) {
            if(fileEntry.isDirectory()) {
                getInputFilenames(fileEntry);
            } else {
                String pdfName = fileEntry.getName();
                if(pdfName.substring(pdfName.length() - 4, pdfName.length()).equals(".pdf")) {
                    inputFileNames.add(fileEntry.getName());
                }
            }
        }
        return inputFileNames;
    }

    /**
     * scrape all of the PDFs in a local directory
     * 
     * @param directory
     *            path to directory
     */
    public static void scrapeDirectory(String directory) {
        ArrayList<String> inputPdfNames = getInputFilenames(new File(directory));
        ParallelScraper pScraper = null;
        for(String pdfName : inputPdfNames) {
            inputPdfName = pdfName;
            pScraper = new ParallelScraper();
            File file = new File(directory + pdfName);
            pScraper.scrape(file);
            resultsToCsv();
        }
    }

    /**
     * Load the scraped information from all PDFs into one new .csv file
     * 
     * @param results
     *            contains the property data for every pdf file processed
     * @param inputPdfNames
     *            arraylist of names of pdfs that were scraped
     */
    public static void resultsToCsv() {
        File outputCsv = new File(outputFilePath + "output.csv");
        StringBuilder sb = new StringBuilder();
        FileWriter pw = null;
        try {
            if(!outputCsv.exists()) {
                outputCsv.createNewFile();
                pw = new FileWriter(new File(outputFilePath + "output.csv"), true);
                sb.append(
                        "PDFName, Scraped Address, Term, Square Footage, Emails, Phone Numbers, Contact Names\n");
            } else {
                pw = new FileWriter(new File(outputFilePath + "output.csv"), true);
            }
            sb.append("\"" + inputPdfName + "\"");
            sb.append(',');
            String entry = ParallelScraper.address.peek() != null ? ParallelScraper.address.peek().getValue(): "**";
            sb.append("\"" + entry + "\"");
            sb.append(',');
            entry = ParallelScraper.term.peek() != null ? ParallelScraper.term.peek().getValue(): "**";
            sb.append("\"" + entry + "\" ");
            sb.append(',');
            entry = ParallelScraper.squareFootage.peek() != null ? ParallelScraper.squareFootage.peek().getValue(): "**";
            sb.append("\"" + entry + "\"");
            sb.append(',');

            String emailAcc = "";
            Entry email;
            while((email = ParallelScraper.emails.poll()) != null) {
                emailAcc+= email.getValue()+"/";
            }
            entry = emailAcc.length() > 0 ? emailAcc : "**,";
            sb.append("\""+entry+"\"");
            sb.append(',');
            String phoneAcc = "";
            Entry phone;
            while((phone = ParallelScraper.phoneNumbers.poll()) != null) {
                phoneAcc += phone.getValue()+" /";
            }
            entry = phoneAcc.length() > 0 ? phoneAcc : "**";
            sb.append("\""+entry+"\"");
            sb.append(',');
            String contactAcc = "";
            Entry contact;
            while((contact = ParallelScraper.contactNames.poll()) != null) {
                contactAcc += contact.getValue()+"/";
            }
            entry = contactAcc.length() > 0 ? contactAcc : "**";
            sb.append("\""+contactAcc+"\"");
            pw.write(sb.append('\n').toString());
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                pw.close();
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }
}
