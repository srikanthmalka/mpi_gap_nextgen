package au.com.michaelpage.gap.nextgen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NextgenExtractor {
	
	private Logger logger = LoggerFactory.getLogger(NextgenExtractor.class);
	
	private static final String AUTHORIZATION = "Basic bWljaGFlbHBhZ2U6NVVDcmVkOGU=";
	
	private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.81 Safari/537.36";

	private final String fileName;
	
	private final String url;
	
	public NextgenExtractor(String url, String fileName) {
		this.url = url;
		this.fileName = fileName;
	}
	
	public long savePageContentToFile() throws Exception {
		URL obj = new URL(url);
		HttpURLConnection conn = (HttpURLConnection) obj.openConnection();

		conn.setRequestMethod("GET");
		conn.setUseCaches(false);
		conn.setRequestProperty("User-Agent", USER_AGENT);
		conn.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		conn.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
		conn.setRequestProperty("Authorization", AUTHORIZATION);
		int responseCode = conn.getResponseCode();
		logger.debug("Sending 'GET' request to URL : <{}>", url);
		logger.debug("Response Code : <{}>", responseCode);

		InputStream input = null; 
		OutputStream output = null;
		
		try {
			byte[] buffer = new byte[8 * 1024];
			input = conn.getInputStream();
			output = new FileOutputStream(fileName);
			  
		    int bytesRead;
		    while ((bytesRead = input.read(buffer)) != -1) {
		    	output.write(buffer, 0, bytesRead);
		    }
		    return new File(fileName).length();
		} finally {
			if (output != null) {
				output.close();
			}
			if (input != null) {
				input.close();
			}
		}			
	}
	
	public static void main(String[] args) throws Exception {
		NextgenExtractor nextgenExtractor = new NextgenExtractor("http://www.michaelpage.co.jp/sites/michaelpage.co.jp/files/job_advert_report/MP_JP_job_advert_json_report.txt",
				"c:\\temp\\MP_JP_job_advert_json_report.txt");
		System.out.println(nextgenExtractor.savePageContentToFile());
		
	}
}
