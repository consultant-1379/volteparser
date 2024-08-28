package com.distocraft.dc5000.etl.volte;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.parser.MeasurementFile;

public class VolteDIM {
	
	private int maxID;
	private ResultSet result;
	private Logger log;
	
	public VolteDIM(){}
	
	public VolteDIM(int maxID, Logger log){
		this.maxID = maxID;
		this.log = log;
	}
	
	public MeasurementFile getERBSTopology(MeasurementFile mFile, RockFactory dbconn, String ossID){
		try{
			result = dbconn.getConnection().createStatement().executeQuery("select OSS_ID, ERBS_FDN, ERBS_NAME, ERBS_VERSION, NEMIMVERSION, VENDOR " +
					"from DIM_E_LTE_ERBS where STATUS='ACTIVE' and OSS_ID = '"+ossID+"'");
						
			while (result.next()) {
				/*HashMap<String, String> nodeDetails = new HashMap<String, String>();
				nodeDetails.put("NODE_NAME", result.getString("ERBS_NAME"));
				nodeDetails.put("FDN", result.getString("ERBS_FDN"));
				//nodeDetails.put("OSS_ID", ossID);
				nodeDetails.put("OSS_ID", result.getString("OSS_ID"));*/

				
				//if(findNode(dbconn, nodeDetails).equals("0")){
					//maxID = maxID + 1;
					//mFile.addData("NODE_ID", ""+maxID);
					mFile.addData("VENDOR", result.getString("VENDOR"));
					mFile.addData("SW_VERSION", result.getString("NEMIMVERSION"));
					mFile.addData("NODE_TYPE", "eNB");
					mFile.addData("NODE_NAME", result.getString("ERBS_NAME"));
					mFile.addData("NODE_VERSION", result.getString("ERBS_VERSION"));
					mFile.addData("FDN", result.getString("ERBS_FDN"));
					mFile.addData("OSS_ID", ossID);
					//mFile.addData("OSS_ID", result.getString("OSS_ID"));
					mFile.addData("SYSTEM_AREA", "RAN");
					mFile.saveData();
				}
				
			//}
			
		}catch(Exception e){
			log.warning("Unable to find node: " + e);
		}finally{
			try {
				if(result != null)
					result.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return mFile;
	}
	
	public MeasurementFile getCNCommonTopology(MeasurementFile mFile, RockFactory dbconn, String node_type, String node_type_opts, String system_area, String node_area, String ossID){
		try{
			String query = "select OSS_ID, NE_FDN, NE_ID, NE_VERSION, VENDOR from DIM_E_CN_CN where STATUS='ACTIVE' and OSS_ID = '"+ossID+"'";
			
			if(!node_type_opts.equals("")){
				query = query + " and (NE_TYPE like '%" + node_type + "%' or NE_TYPE in (" + node_type_opts + "))";
			}
			else{
				query = query + " and NE_TYPE like '%" + node_type + "%'";
			}
			
			result = dbconn.getConnection().createStatement().executeQuery(query);
						
			while (result.next()) {
				/*HashMap<String, String> nodeDetails = new HashMap<String, String>();
				nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
				//nodeDetails.put("OSS_ID",ossID);
				nodeDetails.put("OSS_ID", result.getString("OSS_ID"));*/
				
				//if(findNode(dbconn, nodeDetails).equals("0")){
					//maxID = maxID + 1;
					//mFile.addData("NODE_ID", ""+maxID);
					mFile.addData("VENDOR", result.getString("VENDOR"));
					mFile.addData("NODE_TYPE", node_area);
					mFile.addData("NODE_NAME", result.getString("NE_ID"));
					mFile.addData("NODE_VERSION", result.getString("NE_VERSION"));
					mFile.addData("FDN", result.getString("NE_FDN"));
					mFile.addData("OSS_ID", ossID);
					//mFile.addData("OSS_ID", result.getString("OSS_ID"));
					mFile.addData("SYSTEM_AREA", system_area);
					mFile.saveData();
				}

			//}
			
		}catch(Exception e){
			log.warning("Unable to find node: " + e);
		}finally{
			try {
				if(result != null)
					result.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return mFile;
	}
	
	public MeasurementFile getCNTopology(MeasurementFile mFile, RockFactory dbconn, String table_name, String node_type, String system_area, String ossID){
		try{
			result = dbconn.getConnection().createStatement().executeQuery("select OSS_ID, NE_FDN, NE_ID, NE_VERSION, VENDOR " +
					"from "+ table_name + " where STATUS='ACTIVE' and OSS_ID = '"+ossID+"'");
						
			while (result.next()) {
				/*HashMap<String, String> nodeDetails = new HashMap<String, String>();
				nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
				nodeDetails.put("OSS_ID", ossID);
				//nodeDetails.put("OSS_ID", result.getString("OSS_ID"));
*/				
				//if(findNode(dbconn, nodeDetails).equals("0")){
					//maxID = maxID + 1;
					//mFile.addData("NODE_ID", ""+maxID);
					mFile.addData("VENDOR", result.getString("VENDOR"));
					mFile.addData("NODE_TYPE", node_type);
					mFile.addData("NODE_NAME", result.getString("NE_ID"));
					mFile.addData("NODE_VERSION", result.getString("NE_VERSION"));
					mFile.addData("FDN", result.getString("NE_FDN"));
					mFile.addData("OSS_ID", ossID);
					//mFile.addData("OSS_ID", result.getString("OSS_ID"));
					mFile.addData("SYSTEM_AREA", system_area);
					mFile.saveData();
				}
				
			//}
			
		}catch(Exception e){
			log.warning("Unable to find node: " + e);
		}finally{
			try {
				if(result != null)
					result.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return mFile;
	}
	
	public MeasurementFile getGGSNTopology(MeasurementFile mFile, RockFactory dbconn, String ossID){
		try{
			result = dbconn.getConnection().createStatement().executeQuery("select OSS_ID, NE_ID, NE_FDN, NE_VERSION, VENDOR " +	// Changing ggsnApnName to GGSN
					"from DIM_E_CN_GGSN where STATUS='ACTIVE' and OSS_ID = '"+ossID+"'");															// and DIM_E_CN_GGSN for EQEV-31897
						
			while (result.next()) {
				/*HashMap<String, String> nodeDetails = new HashMap<String, String>();
				nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
//				nodeDetails.put("FDN", result.getString("NE_FDN"));
				//nodeDetails.put("OSS_ID", ossID);
				nodeDetails.put("OSS_ID", result.getString("OSS_ID"));*/
				
				//if(findNode(dbconn, nodeDetails).equals("0")){
					//maxID = maxID + 1;
					//mFile.addData("NODE_ID", ""+maxID);
					mFile.addData("VENDOR", result.getString("VENDOR"));
					mFile.addData("NODE_TYPE", "PGW");
					mFile.addData("NODE_NAME", result.getString("NE_ID"));
					mFile.addData("NODE_VERSION", result.getString("NE_VERSION"));
					mFile.addData("FDN", result.getString("NE_FDN"));
					mFile.addData("OSS_ID", ossID);
					//mFile.addData("OSS_ID", result.getString("OSS_ID"));
					mFile.addData("SYSTEM_AREA", "EPC");
					mFile.saveData();
				}
				
			//}
			
		}catch(Exception e){
			log.warning("Unable to find node: " + e);
		}finally{
			try {
				if(result != null)
					result.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return mFile;
	}
	
	public String findNode(RockFactory dbconn, HashMap<String, String> nodeDetails){		
		ResultSet result = null;
		try{
			String sqlStatement = "select NODE_ID from DIM_E_VOLTE_NODE where ";
			for(String key : nodeDetails.keySet()){
				sqlStatement = sqlStatement + key + " ='" + nodeDetails.get(key) + "' and ";
			}
			sqlStatement = sqlStatement.substring(0, sqlStatement.lastIndexOf("and "));
			result = dbconn.getConnection().createStatement().executeQuery(sqlStatement.trim());
			
			if(result.next()){
				return result.getString("NODE_ID");
			}else{
				return "0";
			}
			
			
		}catch(Exception e){
			System.err.println("Unable to find node: " + e);
		}finally{
			try {
				if(result != null)
					result.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return "0";
	}
	
}

