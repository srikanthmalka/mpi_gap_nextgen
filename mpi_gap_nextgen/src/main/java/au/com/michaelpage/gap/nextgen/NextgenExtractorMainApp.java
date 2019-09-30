package au.com.michaelpage.gap.nextgen;

import java.io.File;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.michaelpage.gap.common.util.Util;

public class NextgenExtractorMainApp {

	private static final Logger logger = LoggerFactory.getLogger(NextgenExtractorMainApp.class);
			
	public static void main(String[] args) throws Exception {
		String outputDir = System.getProperty("outputDir");
		String extractUrl = System.getProperty("extractUrl");
		
		String hostName = extractHostNameFromUrl(extractUrl);

		String baseDir = outputDir + new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "\\";
		String brand = Util.getBrandFromHostName(hostName);
		String country = Util.getCountryCodeFromHostName(hostName);
		String fileName = brand + "_" + country + "_job_advert_json_report.txt";
		
		logger.info("Extract URL: " + extractUrl);
		logger.info("Base dir: " + baseDir);
		logger.info("Country code: " + country);
		logger.info("Brand: " + brand);

		String fullFileName =  baseDir + fileName;
		
		logger.info("File name: " + fullFileName);
		
		if (!checkFolder(baseDir)) {
			System.exit(1);
		}
		
		NextgenExtractor nextgenExtractor = new NextgenExtractor(extractUrl, fullFileName);
		long fileSize = nextgenExtractor.savePageContentToFile();
		
		logger.info("File {} with size {} has been created", fileName, fileSize);
	}
	
	private static String extractHostNameFromUrl(String url) throws Exception {
		URI uri = new URI(url);
	    String hostName = uri.getHost();
	    if (hostName.startsWith("www")) {
	    	hostName = hostName.substring(4);
	    }
		return hostName;
	} 
	
	private static boolean checkFolder(String folder) {
		File file = new File(folder);
		if (file.exists()) {
			if (file.list().length > 0) {
				logger.error("Folder {} already exists and contains files.", folder);
				return false;
			}
		} else {
			file.mkdirs();
		}
		return true;
	}
}
