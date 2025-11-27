package utils;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.ASCIIFoldingFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Utilities {

	public static String getPath() {
		/*String path = System.getProperty("user.dir");
		if (path.endsWith("target")) {
			path = path.substring(0, path.lastIndexOf("/"));
		}*/

		// String path = "C:\\Users\\jonrt\\OneDrive\\Desktop\\Projects\\Age\\data\\";
		String path = "C:/Users/jonrt/OneDrive/Desktop/Projects/Age/data/";
		return path;
	}
	

	public static String CleanStreetName(String street) {
		return street.replaceAll("Block of ", "") 
				.replaceAll("[0-9]+\\s", "")
				.replaceAll("^\\s+", "");
	}
	
	public static Double DayOfWeek(Object dayOfWeek) {
		
		String day = "";
		
		switch (dayOfWeek.toString().toLowerCase()) {
		case "sunday":day = "1"; break;
		case "monday":day = "2"; break;
		case "tuesday":day = "3"; break;
		case "wednesday":day = "4"; break;
		case "thursday":day = "5"; break;
		case "friday":day = "6"; break;
		case "saturday":day = "7"; break;
		default:day = "7"; break;
		}

		return Double.valueOf(day);
	}
	
    public static Double RoundDate(Object date) {
    	String[] dateSplit = date.toString().split("/");
    	
    	String month = "";
    	
    	if(dateSplit.length == 3) {
    		String monthString = dateSplit[0];
    		Integer monthInt = Integer.valueOf(monthString);
 
    		
    		
    		switch (monthInt) {
    		case 1:month = "1"; break;
    		case 2:month = "2"; break;
    		case 3:month = "3"; break;
    		case 4:month = "4"; break;
    		case 5:month = "5"; break;
    		case 6:month = "6"; break;
    		case 7:month = "7"; break;
    		case 8:month = "8"; break;
    		case 9:month = "9"; break;
    		case 10:month = "10"; break;
    		case 11:month = "11"; break;
    		case 12:month = "12"; break;
    		default:month = "0"; break;
    		}
 
    	//	return  month;
    	}
 
 	
    	return Double.valueOf(month);
    }
		
    public static Double RoundTime(Object time) {
    	String[] timeSplit = time.toString().split(":");
    	if(timeSplit.length == 2) {
    		String hourString = timeSplit[0];
    		return Double.valueOf(hourString); //  + ":00";
    	}
    	
    	return Double.valueOf("0");
    }
    
    /*
    public static Object RoundTime1(Object time) {
    	String[] timeSplit = time.toString().split(":");
    	if(timeSplit.length == 2) {
    		String hourString = timeSplit[0];
    		return hourString;
    	}
    	return time;
    }*/
    
    
	public static StructType createSchema(List<String> tableColumns) {
		List<StructField> fields = new ArrayList<StructField>();
		
		
		for (String column : tableColumns) {
			if(column.equals("label")|| column.equals("count")) {
				fields.add(  DataTypes.createStructField(column, DataTypes.IntegerType, false) ) ; 
			} else if(column.equals("DayOfWeek") || column.equals("X")|| column.equals("Y")  || column.equals("Date") || column.equals("Time")) {
				fields.add(  DataTypes.createStructField(column, DataTypes.DoubleType, false) ) ; 
			}else if (column.equals("features")) {
				fields.add(  DataTypes.createStructField(column, new VectorUDT(), false) );
			} 
			
			else 
				fields.add(DataTypes.createStructField(column, DataTypes.StringType, true));
		}
		return DataTypes.createStructType(fields);
	}
	
	public static StructType createSchemaNB(List<String> tableColumns) {
		List<StructField> fields = new ArrayList<StructField>();
 
		for (String column : tableColumns) {
			if(column.equals("label")) {
				fields.add(  DataTypes.createStructField(column, DataTypes.IntegerType, false) ) ; 
			} else 
				fields.add(DataTypes.createStructField(column, DataTypes.StringType, true));
		}
		return DataTypes.createStructType(fields);
	} 
	
	
	public static TokenStream tokenStream(final Reader reader) {
	    TokenStream result = new StandardTokenizer(Version.LUCENE_30, reader);
	    result = new ASCIIFoldingFilter(result);
	    result = new StandardFilter(result);
	    result = new LowerCaseFilter(result);
	    result = new PorterStemFilter(result);
	    return result;
	}

	public static String cleanText(String value) {
		TokenStream stream = tokenStream(new StringReader(value));
		CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);
		StringBuilder builder = new StringBuilder();
		
		try {
			stream.reset();
			while (stream.incrementToken()) {
				String term = charTermAttribute.toString();
				builder.append(term);
				builder.append(" ");
			}
		} catch (Exception e) {

		}

		return  builder.toString() ;
	}  
 
}
