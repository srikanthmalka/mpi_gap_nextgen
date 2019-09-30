package au.com.michaelpage.gap.nextgen.model;

import java.util.Date;

public class JobEntry {
	private Job job;

	public Job getJob() {
		return job;
	}
	
	public class Job {
		private String ref;
		private String language;
		private Location location;
		private String country;
		private String title;
		private Sector sector;
		private Sector subSector;
		private Employer employer;
		private Salary salary;
		private Integer contractType;
		private Date published;
		private LogoImage logoImage;
		private String productType;
		
		public String getRef() {
			return ref;
		}
		
		public String getLanguage() {
			return language;
		}
		
		public Location getLocation() {
			return location;
		}
		
		public String getCountry() {
			return country;
		}

		public String getTitle() {
			return title;
		}

		public Sector getSector() {
			return sector;
		}

		public Sector getSubSector() {
			return subSector;
		}

		public Employer getEmployer() {
			return employer;
		}
		
		public Salary getSalary() {
			return salary;
		}

		public Integer getContractType() {
			return contractType;
		}

		public Date getPublished() {
			return published;
		}

		public LogoImage getLogoImage() {
			return logoImage;
		}

		public String getProductType() {
			return productType;
		}
	}
	
	public class Location {
		private int code;
		private String term;
		
		public int getCode() {
			return code;
		}
		
		public String getTerm() {
			return term;
		}
	}

	public class Sector {
		private String code;
		private String term;
		
		public String getCode() {
			return code;
		}
		
		public String getTerm() {
			return term;
		}
	}

	public class Salary {
		private Long min;
		private Long max;
		
		public Long getMin() {
			return min;
		}
		
		public Long getMax() {
			return max;
		}
	}
	
	public class Employer {
		private String id;
		private String name;
		
		public String getId() {
			return id;
		}
		public String getName() {
			return name;
		}
	}
	
	public class LogoImage {
		private String name;

		public String getName() {
			return name;
		}
	}
}