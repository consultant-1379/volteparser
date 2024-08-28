package com.distocraft.dc5000.etl.volte;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.ericsson.eniq.repository.ETLCServerProperties;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

public class Utils {
	
	public ETLCServerProperties etlcserverprops;
	
	private boolean multiBlade;
	private static Logger log;
	
	public Utils(){
		
	}
	
	public Utils(Logger log) {
		Utils.log = log;
	}
	
	public void loadProperties() throws IOException{
		
		RockFactory etlrep = null;
		Meta_databases repdb_prop;
		Meta_databases dwhdb_prop;
		Meta_databases where_obj;
		Meta_databasesFactory md_fact;
		List<Meta_databases> dbs;
		
		etlcserverprops =  new ETLCServerProperties(System.getProperty(ETLCServerProperties.CONFIG_DIR_PROPERTY_NAME)+"/ETLCServer.properties");
				
		try(Scanner scan = new Scanner(new FileReader(System.getProperty(ETLCServerProperties.CONFIG_DIR_PROPERTY_NAME)+"/service_names"))){	
			while(scan.hasNext()){
				if(scan.next().contains("dwh_reader_2")){
					multiBlade = true;
					break;
				}
			}
		}catch (IOException e) {
			log.warning("Could not find the server type. Server will be considered as standalone.");
		}
		
		try{
			log.finest("Multiblade status --- "+multiBlade);
			
			etlrep = new RockFactory(etlcserverprops.getProperty(ETLCServerProperties.DBURL), etlcserverprops.getProperty(ETLCServerProperties.DBUSERNAME),
					etlcserverprops.getProperty(ETLCServerProperties.DBPASSWORD), etlcserverprops.getProperty(ETLCServerProperties.DBDRIVERNAME),
					"InformationStoreParser",false);
			where_obj = new Meta_databases(etlrep);
			
			//// Getting RepDB Properties			
			where_obj.setType_name("USER");
			where_obj.setConnection_name("dwhrep");
			md_fact = new Meta_databasesFactory(etlrep, where_obj);
			dbs = md_fact.get();
			if (dbs.size() <= 0) {
				throw new RockException("Could not extract repDB log-on details.");
			}
			
			repdb_prop = dbs.get(0);
			//// Setting RepDB Properties
			etlcserverprops.put("repdb_username", repdb_prop.getUsername());
			etlcserverprops.put("repdb_password", repdb_prop.getPassword());
			etlcserverprops.put("repdb_driver", repdb_prop.getDriver_name());
			etlcserverprops.put("dbUrl_repdb", repdb_prop.getConnection_string());
			
			dbs.clear();
			
			if(multiBlade) {
				where_obj.setConnection_name("dwh_reader_2");
			} else {
				where_obj.setConnection_name("dwh");
			}
			
			//// Getting DwhDB Properties
			md_fact = new Meta_databasesFactory(etlrep, where_obj);
			dbs = md_fact.get();
			if (dbs.size() <= 0) {
				throw new RockException("Could not extract dwhDB log-on details.");
			}
			
			dwhdb_prop = dbs.get(0);
			//// Setting DwhDB Properties
			etlcserverprops.put("dwhdb_username", dwhdb_prop.getUsername());
			etlcserverprops.put("dwhdb_password", dwhdb_prop.getPassword());
			etlcserverprops.put("dwhdb_driver", dwhdb_prop.getDriver_name());
			etlcserverprops.put("dbUrl_dwhdb", dwhdb_prop.getConnection_string());
			
			log.config("RepDB Properties: " + etlcserverprops.getProperty("repdb_username")+"; "+etlcserverprops.getProperty("repdb_password")+"; "
					+etlcserverprops.getProperty("repdb_driver")+"; "+etlcserverprops.getProperty("dbUrl_repdb"));
			log.config("DwhDB Properties: " + etlcserverprops.getProperty("dwhdb_username")+"; "+etlcserverprops.getProperty("dwhdb_password")+"; "
					+etlcserverprops.getProperty("dwhdb_driver")+"; "+etlcserverprops.getProperty("dbUrl_dwhdb"));
			
		} catch (SQLException | RockException e) {
			log.warning("Could not get database login details: " + e.getMessage());
		}finally{
			try {
				if (etlrep != null)
					etlrep.getConnection().close();
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}
	}
	
	public void loadParameters(RockFactory dwhdb, String configTable){
		
		ResultSet result = null;
		
		String sql = "select * from "+configTable;
		
		try{
			result = executeQuery(dwhdb, sql);
			while(result.next()){
				etlcserverprops.put(result.getString("PARAMETER"), result.getString("VALUE"));
			}
			log.config(etlcserverprops.toString());
		}catch(Exception e){
			log.log(Level.WARNING, "Could not get parameters");
		}finally{
			try {
				result.close();
			} catch (SQLException e) {
				log.warning("Could not close resultset for parameters. "+ e);
			}
		}
	}
	
	public RockFactory getDBConn(String dbType) throws SQLException, RockException{
		
		if(dbType.contentEquals("dwhdb")){
			return new RockFactory(etlcserverprops.getProperty("dbUrl_dwhdb"), etlcserverprops.getProperty("dwhdb_username"), etlcserverprops.getProperty("dwhdb_password"), etlcserverprops.getProperty("dwhdb_driver"), "NetAnVolteParser", false);
		}else if(dbType.contentEquals("repdb")){
			return new RockFactory(etlcserverprops.getProperty("dbUrl_repdb"), etlcserverprops.getProperty("repdb_username"), etlcserverprops.getProperty("repdb_password"), etlcserverprops.getProperty("repdb_driver"), "NetAnVolteParser", false);
		}
		return null;
	}
	
	public static ResultSet executeQuery(RockFactory dbconn, String sql){
		ResultSet result = null;
		try {
			result = dbconn.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(sql);
		} catch (SQLException e) {	
			log.warning("Could not retrieve data. "+sql);
		}
		return result;
	}
	
	public void createDataFile(String path){
		BufferedWriter bw = null;
		try {
			DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
			Calendar cal = Calendar.getInstance();
			
			String date = dateFormat.format(cal.getTime());

			File file = new File(path, date+".txt");

			if (!file.exists()) {
				file.createNewFile();
			}

			bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
			bw.write(date);
			
		} catch (IOException e) {
			log.log(Level.WARNING, "Unable to write data file", e);
		} finally{
			try {
				if(bw != null)
					bw.close();
			} catch (IOException e) {
				log.warning("Could not close buffered writer.");
			}
		}
	}
	
	public String calculatePreviousRopTime(){
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		Calendar cal = Calendar.getInstance();
		
		int minutes = cal.get(Calendar.MINUTE);
		minutes = minutes - (minutes%5) - 45;
		cal.set(Calendar.MINUTE, minutes);
		cal.clear(Calendar.SECOND);
		return dateFormat.format(cal.getTime());	
	}
	
	public static String getPartitions(RockFactory repdb, String query) throws SQLException{
		
		String tableName = "";
		String queryWithPartition = "";
		String partitionQuery = "";
		ResultSet result = null;
		int count = 0;
		String storageID = "";
		String partition = "";
		
		Pattern p = Pattern.compile("DC_E_.*?_RAW");
		Matcher m = p.matcher(query.toString());
		
		while(m.find()){
			tableName = m.group();
		}
		
		tableName = tableName.replace("_RAW", ":RAW");
		
		partitionQuery = "select * from dwhpartition where CURRENT DATE between starttime and endtime AND STORAGEID in ('"+tableName+"')";
		
		result = Utils.executeQuery(repdb, partitionQuery);
		
		count = 1;
		
		while(result.next()){
			
			storageID = result.getString("STORAGEID").replace(":RAW", "_RAW");
			partition = result.getString("TABLENAME");
			
			if (count > 1){
				queryWithPartition = queryWithPartition.concat(" UNION ");
			}
			
			queryWithPartition += query.replace(storageID, partition);
			
			count++;
			
		}
		
		result.close();
		
		return queryWithPartition;
		
	}

	
}