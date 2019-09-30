package au.com.michaelpage.gap.nextgen;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;
import au.com.michaelpage.gap.common.Settings;
import au.com.michaelpage.gap.common.util.DatabaseManager;
import au.com.michaelpage.gap.common.util.SQLGeneratorHelper;
import au.com.michaelpage.gap.common.util.Util;
import au.com.michaelpage.gap.nextgen.model.JobEntry;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class NextgenImport {
	
	private final String LOCATIONS_MAPPING_FILE = "c:\\GTS\\locations_mapping.csv";
	
	private final String LOCATIONS_MAPPING_TABLE_NAME = "LOCATIONS_MAPPING";
	
	private final Map<Integer, Location> LOCATIONS_CACHE = new HashMap<Integer, Location>();

	private Logger logger = LoggerFactory.getLogger(NextgenImport.class);
	
	private String fileName;
	
	public NextgenImport(String fileName) {
		this.fileName = fileName;
	}
	
	public void importData() throws Exception {
		createTables();
		
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		JsonParser parser = new JsonParser();
		
		ArrayList<JobEntry> jobs = new ArrayList<JobEntry>();
		
		Reader reader = null;
		
		try {
			reader = new FileReader(fileName);
			JsonArray jobArray = parser.parse(reader).getAsJsonArray();
			
			for (JsonElement obj : jobArray) {
				JobEntry jobEntry = gson.fromJson(obj, JobEntry.class);
				jobs.add(jobEntry);
		    }
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
		
		
		String sql = "insert into nextgen_extract (job_id, job_sector_code, job_sector_name, job_subsector_code, job_subsector_name, job_type, job_language, "
				+ "job_location_suburb_code, job_location_suburb_name, job_location_city_code, job_location_city_name, job_location_state_code, "
				+ "job_location_state_name, job_location_country_code, "
				+ "job_location_country_name, job_salary_min, job_salary_max, job_title, job_employer_ref, job_employer_name,"
				+ "job_client_paid, job_first_published_date, job_logo) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"; 
		
		Connection conn = null; 
		PreparedStatement ps = null;
		
		try {
			conn = DatabaseManager.INSTANCE.getConnection();
			
			for (JobEntry jobEntry : jobs) {
				try {
					// Get levels configuration from Settings
					String level1 = Settings.INSTANCE.getOptionValue("nextgen:locationMapping:level1");
					String level2 = Settings.INSTANCE.getOptionValue("nextgen:locationMapping:level2");
					String level3 = Settings.INSTANCE.getOptionValue("nextgen:locationMapping:level3");
					String level4 = Settings.INSTANCE.getOptionValue("nextgen:locationMapping:level4");

					Map<String, Location> locations = lookupLocation(jobEntry.getJob().getLocation().getCode());
					
					if (locations == null) {
						if (10 == jobEntry.getJob().getLocation().getCode() && "International".equals(jobEntry.getJob().getLocation().getTerm())) {
							locations = new HashMap<String, Location>(); 
							locations.put(level1, new Location(jobEntry.getJob().getLocation().getCode(), level1, null,
									jobEntry.getJob().getLocation().getTerm()));
						} else {
							logger.warn("Location {} [{}] not found in the locations mapping list for job {}", 
									jobEntry.getJob().getLocation().getCode(), jobEntry.getJob().getLocation().getTerm(), jobEntry.getJob().getRef());
						}
					}
					
					ps = conn.prepareStatement(sql);
					ps.setString(1, jobEntry.getJob().getRef().replaceAll("A|H|I", ""));
					ps.setString(2, jobEntry.getJob().getSector().getCode());
					ps.setString(3, jobEntry.getJob().getSector().getTerm());
					ps.setString(4, jobEntry.getJob().getSubSector().getCode());
					ps.setString(5, jobEntry.getJob().getSubSector().getTerm());
					ps.setObject(6, jobEntry.getJob().getContractType(), Types.INTEGER);
					ps.setObject(7, jobEntry.getJob().getLanguage(), Types.VARCHAR);
					
					if (locations.get(level4) != null) {
						ps.setInt(8, locations.get(level4).getId());
						ps.setString(9, locations.get(level4).getName());
					} else {
						ps.setNull(8, Types.INTEGER);
						ps.setNull(9, Types.VARCHAR);
					}

					if (locations.get(level3) != null) {
						ps.setInt(10, locations.get(level3).getId());
						ps.setString(11, locations.get(level3).getName());
					} else {
						ps.setNull(10, Types.INTEGER);
						ps.setNull(11, Types.VARCHAR);
					}
					
					if (locations.get(level2) != null) {
						ps.setInt(12, locations.get(level2).getId());
						ps.setString(13, locations.get(level2).getName());
					} else {
						ps.setNull(12, Types.INTEGER);
						ps.setNull(13, Types.VARCHAR);
					}

					if (locations.get(level1) != null) {
						ps.setInt(14, locations.get(level1).getId());
						ps.setString(15, locations.get(level1).getName());
					} else {
						ps.setNull(14, Types.INTEGER);
						ps.setNull(15, Types.VARCHAR);
					}
					
					ps.setObject(16, jobEntry.getJob().getSalary() != null ? jobEntry.getJob().getSalary().getMin() : null, Types.NUMERIC);
					ps.setObject(17, jobEntry.getJob().getSalary() != null ? jobEntry.getJob().getSalary().getMax() : null, Types.NUMERIC);
						
					ps.setString(18, jobEntry.getJob().getTitle());
					ps.setString(19, jobEntry.getJob().getEmployer().getId());
					ps.setString(20, jobEntry.getJob().getEmployer().getName());
					ps.setBoolean(21, jobEntry.getJob().getProductType() != null && jobEntry.getJob().getProductType().equals("1"));
					ps.setTimestamp(22, new java.sql.Timestamp(jobEntry.getJob().getPublished().getTime()));
					ps.setBoolean(23, jobEntry.getJob().getLogoImage() != null && jobEntry.getJob().getLogoImage().getName() != null 
							&& jobEntry.getJob().getLogoImage().getName().length() > 0);
					
					ps.execute();
					ps.close();
				} catch (Exception e) {
					logger.debug("Unable to import json record {}", Util.objectToJson(jobEntry), e);
					throw new RuntimeException(e);
				} finally {
					DatabaseManager.INSTANCE.closeConnection(null, ps, null);
				}
			}
			
		} finally {
			DatabaseManager.INSTANCE.closeConnection(null, null, conn);
		}
		
		// Remove lines with language other than English if English version also exists 
		try {
			conn = DatabaseManager.INSTANCE.getConnection();
			ps = conn.prepareStatement("delete from nextgen_extract e1 where e1.job_language != 'en' "
					+ "and exists (select * from nextgen_extract e2 where e2.job_id = e1.job_id and e2.job_language = 'en')");
			ps.execute();
			ps.close();
		} finally {
			DatabaseManager.INSTANCE.closeConnection(null, ps, conn);
		}
	}
	

	private void createTables() throws Exception {
		
		String sqlCreateExtgenExtractTable = "create table nextgen_extract ("
				+ "job_id varchar(20), "
				+ "job_sector_code varchar(20), "
				+ "job_sector_name varchar(200), "
				+ "job_subsector_code varchar(20), "
				+ "job_subsector_name varchar(200), "
				+ "job_type varchar(20), "
				+ "job_language varchar(20), "
				+ "job_location_suburb_code int, "
				+ "job_location_suburb_name varchar(200), "
				+ "job_location_city_code int, "
				+ "job_location_city_name varchar(200), "
				+ "job_location_state_code int, "
				+ "job_location_state_name varchar(200), "
				+ "job_location_country_code int, "
				+ "job_location_country_name varchar(200), "
				+ "job_salary_min int, "
				+ "job_salary_max int, "
				+ "job_title varchar(500), "
				+ "job_employer_ref varchar(20), "
				+ "job_employer_name varchar(200), "
				+ "job_client_paid boolean, "
				+ "job_first_published_date timestamp, "
				+ "job_logo boolean"
				+ ")";
		
		Connection conn = null;
		PreparedStatement ps = null;
		
		try {
			conn = DatabaseManager.INSTANCE.getConnection();
			ps = conn.prepareStatement(sqlCreateExtgenExtractTable);
			ps.execute();
		} finally {
			DatabaseManager.INSTANCE.closeConnection(null, ps, conn);
		}
		
		
		// Generate location mapping table
		List<String> columns = generateColumnsNames(LOCATIONS_MAPPING_FILE);
		String sqlCreateLocationsMappingTable = SQLGeneratorHelper.generateCreateTable(columns, LOCATIONS_MAPPING_TABLE_NAME);
		
		try {
			conn = DatabaseManager.INSTANCE.getConnection();
			ps = conn.prepareStatement(sqlCreateLocationsMappingTable);
			ps.execute();
		} finally {
			DatabaseManager.INSTANCE.closeConnection(null, ps, conn);
		}
		
		try {
			conn = DatabaseManager.INSTANCE.getConnection();
			ps = conn.prepareStatement("CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (?,?,?,?,?,?,?)");
			ps.setString(1, null);
			ps.setString(2, LOCATIONS_MAPPING_TABLE_NAME);
			ps.setString(3, LOCATIONS_MAPPING_FILE);
			ps.setString(4, ",");
			ps.setString(5, null);
			ps.setString(6, "UTF-8");
			ps.setString(7, "0");
			ps.execute();
		} finally {
			DatabaseManager.INSTANCE.closeConnection(null, ps, conn);
		}
	}
	
	private List<String> generateColumnsNames(String fileName) throws Exception {
		List<String> columns = new ArrayList<String>();
		for (String columnName : new CSVReader(new FileReader(fileName)).readNext()) {
			if (columnName.trim().isEmpty()) continue;
			String s = columnName.replaceAll("[^\\w]", "_").replaceAll("_+", "_");
			if (s.startsWith("_")) {
				s = s.substring(1);
			}
			columns.add(s);
		}
		return columns;
	}
	
	private Map<String, Location> lookupLocation(int id) {
		Map<String, Location> map = new HashMap<String, Location>();
		
		Location location1 = findLocation(id);
		
		// If location is not found, returning empty map
		if (location1 != null) {
			map.put(location1.getType(), location1);
			
			if (location1.getParentId() != null && location1.getParentId() > 0) {
				Location location2 = findLocation(location1.getParentId());
				map.put(location2.getType(), location2);
				
				if (location2.getParentId() != null && location2.getParentId() > 0) {
					Location location3 = findLocation(location2.getParentId());
					map.put(location3.getType(), location3);
					
					if (location3.getParentId() != null && location3.getParentId() > 0) {
						Location location4 = findLocation(location3.getParentId());
						map.put(location4.getType(), location4);
					}
				}
			}
		}
		
		return map;
	}
	
	private Location findLocation(int id) {
		// First check locations cache
		if (LOCATIONS_CACHE.containsKey(id)) {
			return LOCATIONS_CACHE.get(id);
		} else {
			String sql = "select * from locations_mapping where id = ?";

			Connection conn = null; 
			PreparedStatement ps = null;
			ResultSet rs = null;
			
			try {
				conn = DatabaseManager.INSTANCE.getConnection();
				ps = conn.prepareStatement(sql);
				ps.setInt(1, id);
				rs = ps.executeQuery();
				if (rs.next()) {
					Location location = new Location(rs.getInt("id"), rs.getString("type"), rs.getInt("parent_id"), rs.getString("name"));
					LOCATIONS_CACHE.put(location.getId(), location);
					return location;
				} else {
					return null;
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				DatabaseManager.INSTANCE.closeConnection(rs, ps, conn);
			}
		}
		
	}
	
	private class Location {
		private Integer id;
		private String type;
		private Integer parentId;
		private String name;
		
		public Location(Integer id, String type, Integer parentId, String name) {
			this.id = id;
			this.type = type;
			this.parentId = parentId;
			this.name = name;
		}
		
		public Integer getId() {
			return id;
		}
		public String getType() {
			return type;
		}
		public Integer getParentId() {
			return parentId;
		}
		public String getName() {
			return name;
		}
	}
}
