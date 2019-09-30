package au.com.michaelpage.gap.nextgen;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.michaelpage.gap.common.Settings;
import au.com.michaelpage.gap.common.generator.DataOrigin;
import au.com.michaelpage.gap.common.generator.DimensionsGenerator;
import au.com.michaelpage.gap.common.google.GoogleDimensionsUploader;
import au.com.michaelpage.gap.common.google.GoogleUploaderDao;
import au.com.michaelpage.gap.common.util.DatabaseManager;
import au.com.michaelpage.gap.common.util.Util;

import com.google.common.io.Files;

public class NextgenTransformMainApp {

	private static final Logger logger = LoggerFactory.getLogger(NextgenTransformMainApp.class);
	
	private static final String OUTPUT_FOLDER = "C:\\GTS\\Output\\Nextgen\\";
	
	private static final String ARCHIVE_FOLDER = "C:\\GTS\\Archive\\Nextgen\\";
	
	private static final String DATABASE_LOCATION = "c:\\Temp\\GTS_DB_NEXTGEN";
	
	private static final String OUTPUT_FOLDER_FOR_GLOBAL_REGIONAL = "C:\\GTS\\global-regional\\";
	
	public static void main(String[] args) throws Exception {

		logger.info("Started Nextgen Transformations");
		
		int attempts = 0;
		boolean success = false;
		boolean newFilesProcessed = false;

		String incomingFolder = System.getProperty("gts.incomingFolder");
		String hostName = Settings.INSTANCE.getHostName(); 
		String databaseLocation = DATABASE_LOCATION + "\\" + hostName;
		
		logger.info("Hostname: {}", hostName);
		logger.info("Incoming folder: {}", incomingFolder);
		
		
		while (true) {
			attempts++;

			if (attempts > 10 && !success) {
				logger.info("This job hasn't completed successfully after 10 attempts.");
				throw new RuntimeException();
			} else {
				if (success) {
					break;
				}
				
				if (attempts > 1) {
					logger.info("5 minutes pause before attempt #{}.", attempts);
					Thread.sleep(1000*60*5);
				}
				
				try {

					DatabaseManager.INSTANCE.initDatabase(databaseLocation);
					
					Map<Integer, String> outstandingFiles = new GoogleUploaderDao().findOutstandingFiles(); 
					
					if (outstandingFiles.size() == 0) {
						if (newFilesProcessed) {
							logger.info("Finished Nextgen Transformations successfully.");
							break;
						} else {
							logger.info("No incomplete uploads found, proceeding with transformations.");
						}
					} else {
						logger.info("The following {} incomplete uploads have been found, trying to upload them first.", outstandingFiles.size());
						
						for (String fileName : outstandingFiles.values()) {
							logger.info(fileName);
						}
						
						Map<Integer, String> outstandingDimensionFiles = new GoogleUploaderDao().findOutstandingFiles("DIMENSION"); 
						for (String fileName : outstandingDimensionFiles.values()) {
							new GoogleDimensionsUploader().upload(fileName);
						}
						
						outstandingFiles = new GoogleUploaderDao().findOutstandingFiles();
						if (outstandingFiles.size() > 0) {
							logger.info("Incomplete uploads are still found, restarting uploads.");
							continue;
						} else {
							logger.info("Finished Nextgen Transformations successfully.");
							break;
						}
						
					}
				
					DatabaseManager.INSTANCE.shutdownDatabase(false, true);
					
					DatabaseManager.INSTANCE.initDatabase(databaseLocation);

					String currentExtractFolder = resolveIncomingFolder(incomingFolder);

					if (Util.isEmpty(currentExtractFolder)) {
						break;
					}

					String extractDate = currentExtractFolder.substring(currentExtractFolder.lastIndexOf("\\") + 1);
					
					String countryCode = Util.getCountryCodeFromHostName(hostName);
					String brand = Util.getBrandFromHostName(hostName).toUpperCase();

					logger.info("Country code: " + countryCode + ", brand: " + brand);
					
					String timestamp = Util.getTimestamp();
					
					String dimensionsOutputFolder = OUTPUT_FOLDER + hostName + "\\Dimensions\\" + extractDate + "-" + timestamp + "\\";
					
					logger.info("Dimensions output folder: " + dimensionsOutputFolder);
					
					String extractFile = currentExtractFolder + "\\" + brand + "_" + countryCode + "_job_advert_json_report.txt";

					logger.info("Starting processing file " + extractFile);
					
					new NextgenImport(extractFile).importData();
					
					String archivePath = ARCHIVE_FOLDER + hostName + "\\" + extractDate + "-" + timestamp + "\\";
					new File(archivePath).mkdirs();
					
					Files.copy(new File(extractFile), new File(archivePath, new File(extractFile).getName()));
					Util.delete(extractFile);
					
					Util.delete(currentExtractFolder);

					// Generate dimensions
					new DimensionsGenerator(dimensionsOutputFolder, OUTPUT_FOLDER_FOR_GLOBAL_REGIONAL, timestamp).generate(DataOrigin.NEXTGEN);
					
					newFilesProcessed = true;
					
					//Upload dimensions
					File dimensionsFolder = new File(dimensionsOutputFolder);
					for (File file : dimensionsFolder.listFiles()) {
						new GoogleDimensionsUploader().upload(file.getCanonicalPath());
					}
					
					outstandingFiles = new GoogleUploaderDao().findOutstandingFiles(); 
					if (outstandingFiles.size() > 0) {
						success = false;
					} else {
						success = true;
						logger.info("Finished Nextgen Transformations successfully.");
					}
					
				} catch (Throwable t) {
					logger.error("An error occured, this job hasn't completed successfully. Please check log files for details");
					logger.debug(t.getMessage(), t);
				} finally {
					DatabaseManager.INSTANCE.shutdownDatabase(false, false);
				}
			}
		}
				
	}
	
	private static String resolveIncomingFolder(String base) throws IOException {
		File folder = new File(base);
		File[] files = folder.listFiles();
		
		if (files != null && files.length > 0) {
			Arrays.sort(files, new Comparator<File>() {
				@Override
				public int compare(File f1, File f2) {
					try {
						return f1.getCanonicalPath().compareTo(f2.getCanonicalPath());
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			});
			return files[0].getCanonicalPath();
		} else {
			logger.error("Incoming folder " + base + " doesn't contain any extracts.");
			return null;
		}
	}
	
	
}
