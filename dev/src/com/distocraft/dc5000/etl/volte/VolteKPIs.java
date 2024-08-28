package com.distocraft.dc5000.etl.volte;

import java.util.List;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.parser.MeasurementFile;

public class VolteKPIs {

	private VolteDIM voltedim;
	private HashMap<String, String> thresholds;
	private Logger log;
	private ResultSet result;
	private RockFactory repdb;
	
	
	public VolteKPIs(Logger log, HashMap<String, String> thresholds, RockFactory repdb){
		voltedim = new VolteDIM();
		this.log = log;
		this.repdb = repdb;
		this.thresholds = thresholds;
	}
	
	private boolean checkActiveTP(List<String> activeTPs, int kpi_id){
		switch(kpi_id){
		case 1: case 2: case 8: case 11: case 18: case 19: case 20: case 22: case 23: case 26: case 27: case 28:
			if(activeTPs.contains("DC_E_IMSGW_SBG"))
				return true;
			return false;
		case 6:
			if(activeTPs.contains("DC_E_GGSN"))
				return true;
			return false;
		case 14: case 15: case 16: case 17:
			if(activeTPs.contains("DC_E_CNAXE"))
				return true;
			return false;
		case 3: case 4: case 5: case 9: case 10: case 25: 
			if(activeTPs.contains("DC_E_MTAS"))
				return true;
			return false;
		case 7:
			if(activeTPs.contains("DC_E_CSCF"))
				return true;
			return false;
		case 12: case 13: case 21: case 34: case 35:
			if(activeTPs.contains("DC_E_SGSNMME"))
				return true;
			return false;
		default: return false;
		
		}
	}
	
	public MeasurementFile KPI01(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 1");
			if(checkActiveTP(activeTPs, 1)){
				String sql = "select NE_ID, DATETIME_ID, 100*avg(sbgSipNetIncSessionEstabNetworkSuccess/sbgSipTotalIncSessSetups) as 'KPI_VALUE' " +
						"from  DC_E_IMSSBG_SIP_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' GROUP BY NE_ID, DATETIME_ID";
				
				sql = Utils.getPartitions(repdb, sql);

				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					
					try{
						BigDecimal KPI_Value = new BigDecimal(result.getFloat("KPI_Value"));
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("1", KPI_Value);
						}
												
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "1");
						//mFile.addData("KPI_VALUE", "" + KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}
					catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_IMSGW_SBG is not installed.");				
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 1: ", e);
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
	
	public MeasurementFile KPI02(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 2");
			if(checkActiveTP(activeTPs, 2)){
				
				String sql = "select NE_ID, DATETIME_ID, 100*avg(sbgSipNetOutSessionEstabNetworkSuccess/sbgSipTotalOutSessSetups) as 'KPI_VALUE'"
						+ " from  DC_E_IMSSBG_SIP_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' GROUP BY NE_ID, DATETIME_ID";
				
				sql = Utils.getPartitions(repdb, sql);
				
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					
					try{
						BigDecimal KPI_Value = new BigDecimal(result.getFloat("KPI_VALUE"));
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("2", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "2");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
						
					}
				}
			}else{
				log.warning("DC_E_IMSGW_SBG is not installed.");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 2: ", e);
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
	
	public MeasurementFile KPI03(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 3");
			if(checkActiveTP(activeTPs, 3)){
				
				String sql = "select MOID, DATETIME_ID, g3ManagedElement, MtasMmtOrigNetworkSuccessSessionEstablish, " +
						"MtasMmtOrigUnregNetworkSuccessSessionEstablish, MtasMmtOrigFailedAttemptCause, MtasMmtOrigUnregFailedAttemptCause, MtasMmtOrigFailedAttempt, MtasMmtOrigUnregFailedAttempt"+
						" from  DC_E_MTAS_MTASMMT_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";
				
				sql = Utils.getPartitions(repdb, sql);
				
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				
				HashMap<String, HashMap<String, String>> KPIs = new HashMap<String, HashMap<String, String>>();
				Pattern pattern = Pattern.compile(VolteERBSUtils.IPADDRESS_PATTERN);
				while (result.next()) {
					String ME = result.getString("g3ManagedElement");
					String moid = result.getString("MOID");
					String oss_id = ossID;
					String nodeID = ME + "::" + oss_id;
					
					float counter1 = 0;
					float counter2 = 0;
					float counter3 = 0;
					
					if(pattern.matcher(moid).matches()){
						counter3 = result.getFloat("MtasMmtOrigFailedAttempt") + result.getFloat("MtasMmtOrigUnregFailedAttempt");
					}else if(moid.contains("403") || moid.contains("404") || moid.contains("407") || moid.contains("484") || moid.contains("486") || moid.contains("600")){
						counter1 = result.getFloat("MtasMmtOrigFailedAttemptCause") + result.getFloat("MtasMmtOrigUnregFailedAttemptCause");
					}else{
						try{
							int moidValue = Integer.valueOf(moid);
							if(180 <= moidValue && moidValue <= 204){
								counter1 = result.getFloat("MtasMmtOrigNetworkSuccessSessionEstablish") + result.getFloat("MtasMmtOrigUnregNetworkSuccessSessionEstablish");
								counter2 = result.getFloat("MtasMmtOrigNetworkSuccessSessionEstablish") + result.getFloat("MtasMmtOrigUnregNetworkSuccessSessionEstablish");
							}
						}catch(Exception e){
							//Suppress exception where MOID could not be parsed to an integer
							//MOID is not valid structure for this counter summation. 
						}
					}
					
	
					if(KPIs.containsKey(nodeID)){
						HashMap<String, String> values = KPIs.get(nodeID);
						
						counter1 = counter1 + Float.parseFloat(values.get("counter1"));
						counter2 = counter2 + Float.parseFloat(values.get("counter2"));
						counter3 = counter3 + Float.parseFloat(values.get("counter3"));
						values.put("counter1", ""+counter1);
						values.put("counter2", ""+counter2);
						values.put("counter3", ""+counter3);
						
						KPIs.put(nodeID, values);
						
					}else{
						HashMap<String, String> values = new HashMap<String, String>();
						values.put("g3ManagedElement", ME);
						values.put("MOID", moid);
						values.put("OSS_ID", ossID);
						values.put("DATETIME_ID", result.getString("DATETIME_ID"));
						values.put("counter1", ""+counter1);
						values.put("counter2", ""+counter2);
						values.put("counter3", ""+counter3);
						KPIs.put(nodeID, values);
					}
	
				}
				
				for(String nodeID : KPIs.keySet()){
					HashMap<String, String> values = KPIs.get(nodeID);
					
					float counter1 = Float.parseFloat(values.get("counter1"));
					float counter2 = Float.parseFloat(values.get("counter2"));
					float counter3 = Float.parseFloat(values.get("counter3"));
					
					BigDecimal KPI_Value = null;
					String breach = null;
					try{
						KPI_Value = new BigDecimal( 100*(counter1/(counter2 + counter3)) );
						KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
						breach = calculateBreach("3", KPI_Value);
						mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
						log.log(Level.FINEST, "Counter values were : counter1="+ counter1 + ", counter2=" + counter2 + ", counter3=" + counter3 );
					}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", values.get("g3ManagedElement"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", values.get("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "3");
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
						





				}
			}else{
				log.warning("DC_E_MTAS is not installed.");
			}

		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 3: ", e);
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
	
	public MeasurementFile KPI04(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 4");
			if(checkActiveTP(activeTPs, 4)){

				String sql = "select g3ManagedElement, DATETIME_ID, MOID, MtasMmtTermNetworkSuccessSessionEstablish, " +
						"MtasMmtTermUnregNetworkSuccessSessionEstablish, MtasMmtTermFailedAttemptCause, MtasMmtTermUnregFailedAttemptCause, MtasMmtTermFailedAttempt, MtasMmtTermUnregFailedAttempt"+
						" from  DC_E_MTAS_MTASMMT_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";
				
				sql = Utils.getPartitions(repdb, sql);
				
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				HashMap<String, HashMap<String, String>> KPIs = new HashMap<String, HashMap<String, String>>();
				Pattern pattern = Pattern.compile(VolteERBSUtils.IPADDRESS_PATTERN);
				while (result.next()) {
					String ME = result.getString("g3ManagedElement");
					String moid = result.getString("MOID");
					String oss_id = ossID;
					String nodeID = ME + "::" + oss_id;
			        
			        float counter1 = 0;
			        float counter2 = 0;
			        float counter3 = 0;
			        
			        if(pattern.matcher(moid).matches()){
						counter3 = result.getFloat("MtasMmtTermFailedAttempt") + result.getFloat("MtasMmtTermUnregFailedAttempt");
					}else if(moid.contains("403") || moid.contains("404") || moid.contains("407") || moid.contains("484") || moid.contains("486") || moid.contains("600")){
						counter1 = result.getFloat("MtasMmtTermFailedAttemptCause") + result.getFloat("MtasMmtTermUnregFailedAttemptCause");
					}else{
						try{
							int moidValue = Integer.valueOf(moid);
							if(180 <= moidValue && moidValue <= 204){
								counter1 = result.getFloat("MtasMmtTermNetworkSuccessSessionEstablish") + result.getFloat("MtasMmtTermUnregNetworkSuccessSessionEstablish");
								counter2 = result.getFloat("MtasMmtTermNetworkSuccessSessionEstablish") + result.getFloat("MtasMmtTermUnregNetworkSuccessSessionEstablish");
							}
						}catch(Exception e){
							//Suppress exception where MOID could not be parsed to an integer
							//MOID is not valid structure for this counter summation. 
						}
					}
			        
			        if(KPIs.containsKey(nodeID)){
			            HashMap<String, String> values = KPIs.get(nodeID);
			            
			            counter1 = counter1 + Float.parseFloat(values.get("counter1"));
						counter2 = counter2 + Float.parseFloat(values.get("counter2"));
						counter3 = counter3 + Float.parseFloat(values.get("counter3"));
						values.put("counter1", ""+counter1);
						values.put("counter2", ""+counter2);
						values.put("counter3", ""+counter3);
			            
			            KPIs.put(nodeID, values);
			            
			          }else{
			            HashMap<String, String> values = new HashMap<String, String>();
			            values.put("g3ManagedElement", ME);
			            values.put("MOID", moid);
			            values.put("OSS_ID", ossID);
			            values.put("DATETIME_ID", result.getString("DATETIME_ID"));
			            values.put("counter1", ""+counter1);
						values.put("counter2", ""+counter2);
						values.put("counter3", ""+counter3);
			            KPIs.put(nodeID, values);
			          }
				}
				
				for(String nodeID : KPIs.keySet()){
			        HashMap<String, String> values = KPIs.get(nodeID);
			        
			        float counter1 = Float.parseFloat(values.get("counter1"));
			        float counter2 = Float.parseFloat(values.get("counter2"));
			        float counter3 = Float.parseFloat(values.get("counter3"));
			        
			        BigDecimal KPI_Value = null;
			        String breach = null;
			        try{
				        KPI_Value = new BigDecimal( 100*(counter1/(counter2 + counter3)) );
						KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				        breach = calculateBreach("4", KPI_Value);
				        mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				        mFile.addData("BREACH_INDICATION", breach);
			        }catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
						log.log(Level.FINEST, "Counter values were : counter1="+ counter1 + ", counter2=" + counter2 + ", counter3=" + counter3 );
					}
				        
				        HashMap<String, String> nodeDetails = new HashMap<String, String>();
				        nodeDetails.put("NODE_NAME", values.get("g3ManagedElement"));
				        nodeDetails.put("OSS_ID", ossID);
				        
				        mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
				        mFile.addData("UTC_DATETIME_ID", datestamp);
				        mFile.addData("DATETIME_ID", values.get("DATETIME_ID"));
				        mFile.addData("TIMELEVEL", "15MIN");
				        mFile.addData("PERIOD_DURATION", "15");
				        mFile.addData("DATE_ID", datestamp.split(" ")[0]);
				        mFile.addData("KPI_ID", "4");
				        mFile.addData("OSS_ID", ossID);
				        mFile.saveData();
			        




			      }
			}else{
				log.warning("DC_E_MTAS is not installed.");
			}

		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 4: ", e);
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
	
	public MeasurementFile KPI05(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		ResultSet result1 = null;
		try{
			log.log(Level.FINE, "Running KPI 5");
			if(checkActiveTP(activeTPs, 5)){
			
				String sql1 = "select g3ManagedElement, DATETIME_ID, MtasSccInitOrigSessCsNOkE, MtasSccInitOrigSessCsNOkI, MtasSccInitOrigSessCsOk, "+
						"MtasSccInitOrigUnregSessCsNOkE, MtasSccInitOrigUnregSessCsNOkI, MtasSccInitOrigUnregSessCsOk FROM  DC_E_MTAS_MTASSCC_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";
				sql1 = Utils.getPartitions(repdb, sql1);
				result = dbconn.getConnection().createStatement().executeQuery(sql1);
				
				String sql2 = "select g3ManagedElement, DATETIME_ID, MtasSdsCapInitDPNOkE, MtasSdsCapInitDPNOkI, " +
						"MtasSdsCapInitDPOk FROM  DC_E_MTAS_MTASSDS_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";
				sql2 = Utils.getPartitions(repdb, sql2);
				result1 = dbconn.getConnection().createStatement().executeQuery(sql2);
				
				HashMap<String, HashMap<String, String>> KPIs = new HashMap<String, HashMap<String, String>>();
				while (result.next()) {
					String name = result.getString("g3ManagedElement");
			        String nodeID = name + ":" + ossID;
			        
		            HashMap<String, String> values = new HashMap<String, String>();
		            values.put("g3ManagedElement", name);
		            values.put("OSS_ID", ossID);
		            values.put("DATETIME_ID", result.getString("DATETIME_ID"));
		            values.put("MtasSccInitOrigSessCsNOkE", ""+result.getFloat("MtasSccInitOrigSessCsNOkE"));
		            values.put("MtasSccInitOrigSessCsNOkI", ""+result.getFloat("MtasSccInitOrigSessCsNOkI"));
		            values.put("MtasSccInitOrigSessCsOk", ""+result.getFloat("MtasSccInitOrigSessCsOk"));
		            values.put("MtasSccInitOrigUnregSessCsNOkE", ""+result.getFloat("MtasSccInitOrigUnregSessCsNOkE"));
		            values.put("MtasSccInitOrigUnregSessCsNOkI", ""+result.getFloat("MtasSccInitOrigUnregSessCsNOkI"));
		            values.put("MtasSccInitOrigUnregSessCsOk", ""+result.getFloat("MtasSccInitOrigUnregSessCsOk"));
		            KPIs.put(nodeID, values);
				}
				
				while (result1.next()) {
					String name = result1.getString("g3ManagedElement");
			        String nodeID = name + ":" + ossID;
			        
			        if(KPIs.containsKey(nodeID)){
				        HashMap<String, String> values = KPIs.get(nodeID);
			            values.put("MtasSdsCapInitDPNOkE", ""+result1.getFloat("MtasSdsCapInitDPNOkE"));
			            values.put("MtasSdsCapInitDPNOkI", ""+result1.getFloat("MtasSdsCapInitDPNOkI"));
			            values.put("MtasSdsCapInitDPOk", ""+result1.getFloat("MtasSdsCapInitDPOk"));
			            KPIs.put(nodeID, values);
			        }
				}
				
				for(String nodeID : KPIs.keySet()){
					HashMap<String, String> values = KPIs.get(nodeID);
					
					float counter1 = Float.parseFloat(values.get("MtasSccInitOrigSessCsNOkE"));
			        float counter2 = Float.parseFloat(values.get("MtasSccInitOrigSessCsNOkI"));
			        float counter3 = Float.parseFloat(values.get("MtasSccInitOrigSessCsOk"));
			        float counter4 = Float.parseFloat(values.get("MtasSccInitOrigUnregSessCsNOkE"));
			        float counter5 = Float.parseFloat(values.get("MtasSccInitOrigUnregSessCsNOkI"));
			        float counter6 = Float.parseFloat(values.get("MtasSccInitOrigUnregSessCsOk"));
			        float counter7 = Float.parseFloat(values.get("MtasSdsCapInitDPNOkE"));
			        float counter8 = Float.parseFloat(values.get("MtasSdsCapInitDPNOkI"));
			        float counter9 = Float.parseFloat(values.get("MtasSdsCapInitDPOk"));
			        
			        BigDecimal KPI_Value = null;
			        String breach = null;
			        try{
				        KPI_Value = new BigDecimal( 100*((counter3+counter6) / (counter1 + counter2 + counter3 + counter4 + counter5 + counter6) * (counter9 / (counter9 + counter8 + counter7))) );
						KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				        breach = calculateBreach("5", KPI_Value);
				        mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				        mFile.addData("BREACH_INDICATION", breach);
			        }catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
						log.log(Level.FINEST, "Counter values were : counter1="+ counter1 + ", counter2=" + counter2 + ", counter3=" + counter3 + 
								", counter4="+ counter4 + ", counter5=" + counter5 + ", counter6=" + counter6);
					}
						
				        HashMap<String, String> nodeDetails = new HashMap<String, String>();
				        nodeDetails.put("NODE_NAME", values.get("g3ManagedElement"));
				        nodeDetails.put("OSS_ID", ossID);
				        
				        mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
				        mFile.addData("UTC_DATETIME_ID", datestamp);
				        mFile.addData("DATETIME_ID", values.get("DATETIME_ID"));
				        mFile.addData("TIMELEVEL", "15MIN");
				        mFile.addData("PERIOD_DURATION", "15");
				        mFile.addData("DATE_ID", datestamp.split(" ")[0]);
				        mFile.addData("KPI_ID", "5");
				        mFile.addData("OSS_ID", ossID);
				        mFile.saveData();
			        




				}
			}else{
				log.warning("DC_E_MTAS is not installed");
			}
	
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 5: ", e);
		}finally{
			try {
				if(result != null)
					result.close();
				if(result1 != null)
					result1.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return mFile;
	}
	
	public MeasurementFile KPI06(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 6");
			if(checkActiveTP(activeTPs, 6)){
			
				String sql = "select ggsn, DATETIME_ID, avg((pgwApnAttemptedEpsBearerActivation - pgwApnCompletedEpsBearerActivation)/pgwApnAttemptedEpsBearerActivation) as KPI_VALUE " +
						"from  DC_E_GGSN_APN_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' group by ggsn, DATETIME_ID";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				while (result.next()) {
					
					try{
						BigDecimal KPI_Value = new BigDecimal(result.getFloat("KPI_VALUE") );
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("6", KPI_Value);
						}
												
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("ggsn"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "6");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
						
					}
				}
			}else{
				log.warning("DC_E_GGSN is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 6: ", e);
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
	
	public MeasurementFile KPI07(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 7");
			if(checkActiveTP(activeTPs, 7)){
				HashMap<String, HashMap<String, String>> KPIs = new HashMap<String, HashMap<String, String>>();
				String sql1 = "select NE_NAME, DATETIME_ID, MOID, scscfThirdPartyRegistrationSuccess , " +
						"scscfThirdPartyRegistrationFailure from  DC_E_CSCF_REG_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";
				sql1 = Utils.getPartitions(repdb, sql1);
				result = dbconn.getConnection().createStatement().executeQuery(sql1);
				
				KPIs.putAll(KPI07_combination(result, "NE_NAME", ossID));
				
				String sql2 = "select g3ManagedElement, DATETIME_ID, MOID, scscfThirdPartyRegistrationSuccess , " +
						"scscfThirdPartyRegistrationFailure from  DC_E_IMS_CSCF2_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";
				sql2 = Utils.getPartitions(repdb, sql2);
				result = dbconn.getConnection().createStatement().executeQuery(sql2);
				
				KPIs.putAll(KPI07_combination(result, "g3ManagedElement", ossID));
	
			    for(String nodeID : KPIs.keySet()){
					HashMap<String, String> values = KPIs.get(nodeID);
					
					float counter1 = Float.parseFloat(values.get("counter1"));
					float counter2 = Float.parseFloat(values.get("counter2"));
					
					BigDecimal KPI_Value = null;
					String breach = null;
					try{
						KPI_Value = new BigDecimal( (100*counter1)/(counter2) );
						KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
						breach = calculateBreach("7", KPI_Value);
						mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
						log.log(Level.FINEST, "Counter values were : counter1="+ counter1 + ", counter2=" + counter2);
					}
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", values.get("NE_NAME"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", values.get("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "7");
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					




				}
			}else{
				log.warning("DC_E_CSCF is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 7: ", e);
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
	
	private HashMap<String, HashMap<String, String>> KPI07_combination(ResultSet result, String nodeColumn, String ossID){
		HashMap<String, HashMap<String, String>> KPIs = new HashMap<String, HashMap<String, String>>();
		try{
		    while (result.next()) {
				String NE = result.getString(nodeColumn);
				String moid = result.getString("MOID");
				String OSS_ID = ossID;
				String nodeID = NE + "::" + OSS_ID;
	
				float counter1 = result.getFloat("scscfThirdPartyRegistrationSuccess");
				float counter2 = result.getFloat("scscfThirdPartyRegistrationSuccess");
	
				if (moid.equalsIgnoreCase("sum")) {
					counter2 = counter2 + result.getFloat("scscfThirdPartyRegistrationFailure");
				}else if(moid.contains("401")){
					counter2 = counter2 - result.getFloat("scscfThirdPartyRegistrationFailure");
				}
	
				if (KPIs.containsKey(nodeID)) {
					HashMap<String, String> values = KPIs.get(nodeID);
	
					counter1 = counter1+ Float.parseFloat(values.get("counter1"));
					counter2 = counter2+ Float.parseFloat(values.get("counter2"));
					values.put("counter1", "" + counter1);
					values.put("counter2", "" + counter2);
	
					KPIs.put(nodeID, values);
	
				} else {
					HashMap<String, String> values = new HashMap<String, String>();
					values.put("NE_NAME", NE);
					values.put("MOID", moid);
					values.put("DATETIME_ID", result.getString("DATETIME_ID"));
					values.put("OSS_ID", ossID);
					values.put("counter1", "" + counter1);
					values.put("counter2", "" + counter2);
					KPIs.put(nodeID, values);
				}
	
			}
		}catch (Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 7: ", e);
		}
		return KPIs;
		
	}
	
	
	public MeasurementFile KPI08(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 8");
			if(checkActiveTP(activeTPs, 8)){
				String sql = "select NE_ID, DATETIME_ID, 100*avg((sbgSipRegStatInitialAccCnt + sbgSipRegStatRejInitialRegCnt400 + sbgSipRegStatRejInitialRegCnt403)/sbgSipRegStatInitialAttCnt) as KPI_VALUE " +
						" from  DC_E_IMSSBG_PROXYREGISTRAR_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' group by NE_ID, DATETIME_ID";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					
					try{
						BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_VALUE" ));
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("8", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "8");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_IMSGW_SBG is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 8: ", e);
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
	
	public MeasurementFile KPI09(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 9");
			if(checkActiveTP(activeTPs, 9)){
				String sql = "select g3ManagedElement, DATETIME_ID, MtasFuncTermOrigSessOk " +
						", MtasFuncTermOrigSessNOkI, MtasFuncTermOrigSessNOkE from  DC_E_MTAS_MTASQOS_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";
				
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					float counter1 = result.getFloat("MtasFuncTermOrigSessOk");
					float counter2 = result.getFloat("MtasFuncTermOrigSessNOkI");
					float counter3 = result.getFloat("MtasFuncTermOrigSessNOkE");
					
					BigDecimal KPI_Value = null; 
					String breach = null;
					try{
						KPI_Value = new BigDecimal( 100*((counter1 + counter3)/(counter1 + counter2 + counter3)) );
						KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
						breach = calculateBreach("9", KPI_Value);
						mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
						log.log(Level.FINEST, "Counter values were : counter1="+ counter1 + ", counter2=" + counter2 + ", counter3=" + counter3 );
					}
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("g3ManagedElement"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "9");
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					




				}
			}else{
				log.warning("DC_E_MTAS is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 9: ", e);
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
	
	public MeasurementFile KPI10(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 10");
			if(checkActiveTP(activeTPs, 10)){
				String sql = "select g3ManagedElement, DATETIME_ID, MtasFuncTermTermSessOk " +
						", MtasFuncTermTermSessNOkI, MtasFuncTermTermSessNOkE from  DC_E_MTAS_MTASQOS_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					float counter1 = result.getFloat("MtasFuncTermTermSessOk");
					float counter2 = result.getFloat("MtasFuncTermTermSessNOkI");
					float counter3 = result.getFloat("MtasFuncTermTermSessNOkE");
					
					BigDecimal KPI_Value = null; 
					String breach = null;
					try{
						KPI_Value = new BigDecimal( 100*((counter1 + counter3)/(counter1 + counter2 + counter3)) );
						KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
						breach = calculateBreach("10", KPI_Value);
						mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
						log.log(Level.FINEST, "Counter values were : counter1="+ counter1 + ", counter2=" + counter2 + ", counter3=" + counter3 );
					}
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("g3ManagedElement"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "10");
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					




				}
			}else{
				log.warning("DC_E_MTAS is not installed.");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 10: ", e);
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
	
	public MeasurementFile KPI11(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 11");
			if(checkActiveTP(activeTPs, 11)){
				String sql = "select NE_ID, DATETIME_ID, 100*avg((sbgSipSuccessIncSessions - sbgSipRejectedIncAlertingSessions488 - sbgSipRejectedIncPreAlertingSessions488)/sbgSipTotalIncSessSetups) as KPI_VALUE "
						+ "from  DC_E_IMSSBG_SIP_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' GROUP BY NE_ID, DATETIME_ID";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {					
	
					try{
						BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_VALUE"));
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("11", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "11");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_IMSGW_SBG is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 11: ", e);;
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
	
	public MeasurementFile KPI12(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 12");
			if(checkActiveTP(activeTPs, 12)){
				/*String sql = "select SGSN, DATETIME_ID, VS_MM_SrvccCsOnlyToWAtt_E " +
						", VS_MM_SrvccCsOnlyToWSucc_E, VS_MM_SrvccCsAndPsToWAtt_E, VS_MM_SrvccCsAndPsToWSucc_E, VS_MM_SrvccCsAndPsToWCsSuccPsFailed_E" +
						" from  DC_E_SGSNMME_MOBILITY_MM_E_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";*/
				
				String sql = "select SGSN, DATETIME_ID, "
						+ "isnull((VS_MM_SrvccCsOnlyToWAtt_E - VS_MM_SrvccCsOnlyToWSucc_E + VS_MM_SrvccCsAndPsToWAtt_E - VS_MM_SrvccCsAndPsToWSucc_E - VS_MM_SrvccCsAndPsToWCsSuccPsFailed_E)/(VS_MM_SrvccCsOnlyToWAtt_E + VS_MM_SrvccCsAndPsToWAtt_E),"
						+ "(VS_MM_SrvccCsOnlyToWAtt_E - VS_MM_SrvccCsOnlyToWSucc_E)/(VS_MM_SrvccCsOnlyToWAtt_E),"
						+ "(VS_MM_SrvccCsAndPsToWAtt_E - VS_MM_SrvccCsAndPsToWSucc_E - VS_MM_SrvccCsAndPsToWCsSuccPsFailed_E)/(VS_MM_SrvccCsAndPsToWAtt_E)) as KPI_Value "
						+ "from  DC_E_SGSNMME_MOBILITY_MM_E_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";
				
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					/*float counter1 = result.getFloat("VS_MM_SrvccCsOnlyToWAtt_E");
					float counter2 = result.getFloat("VS_MM_SrvccCsOnlyToWSucc_E");
					float counter3 = result.getFloat("VS_MM_SrvccCsAndPsToWAtt_E");
					float counter4 = result.getFloat("VS_MM_SrvccCsAndPsToWSucc_E");
					float counter5 = result.getFloat("VS_MM_SrvccCsAndPsToWCsSuccPsFailed_E");*/
	
					try{
						//BigDecimal KPI_Value = new BigDecimal( (counter1 - counter2 + counter3 - counter4 - counter5)/(counter1 + counter3) );
						BigDecimal KPI_Value = new BigDecimal(result.getFloat("KPI_Value"));
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("12", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("SGSN"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "12");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_SGSNMME is not installed.");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 12: ", e);
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
	
	public MeasurementFile KPI13(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 13");
			if(checkActiveTP(activeTPs, 13)){
				/*String sql = "select SGSN, DATETIME_ID, VS_MM_SrvccCsOnlyToGSucc_E " +
						", VS_MM_SrvccCsOnlyToGAtt_E from  DC_E_SGSNMME_MOBILITY_MM_E_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";*/
				
				String sql = "select SGSN, DATETIME_ID, "
						+ "(VS_MM_SrvccCsOnlyToGAtt_E - VS_MM_SrvccCsOnlyToGSucc_E)/(VS_MM_SrvccCsOnlyToGAtt_E) as KPI_Value "
						+ "from  DC_E_SGSNMME_MOBILITY_MM_E_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";
				
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					/*float counter1 = result.getFloat("VS_MM_SrvccCsOnlyToGAtt_E");
					float counter2 = result.getFloat("VS_MM_SrvccCsOnlyToGSucc_E");*/
	
					try{
						//BigDecimal KPI_Value = new BigDecimal( (counter1 - counter2)/(counter1) );
						BigDecimal KPI_Value = new BigDecimal(result.getFloat("KPI_Value"));
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("13", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("SGSN"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "13");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_SGSNMME is not installed.");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 13: ", e);
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
	
	public MeasurementFile KPI14(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 14");
			if(checkActiveTP(activeTPs, 14)){
				String sql = "select oss_id, sn, DATETIME_ID, left(elem, (case when charindex(',', ELEM)=0 then length(elem) when charindex(',', ELEM)>0 then charindex(',', ELEM)-1 end)) as ELEM,"
						+ " 100*avg(P2CCOMPLACK/P2CREQUTOT) as KPI_VALUE " + 
						"from DC_E_CNAXE_MMESTAT_RAW  " + 
						"where UTC_DATETIME_ID='" +datestamp +"' and statistics_type <> 'CLUSTER' and OSS_ID='" +ossID +"' " + 
						"group by oss_id, sn, elem, DATETIME_ID";
				
				sql = Utils.getPartitions(repdb, sql);
				
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
	
					try{
						BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_Value") );
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("14", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("ELEM"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "14");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_CNAXE is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 14: ", e);
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
	
	public MeasurementFile KPI15(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 15");
			if(checkActiveTP(activeTPs, 15)){
				String sql = "select DATETIME_ID, left(elem, (case when charindex(',', ELEM)=0 then length(elem) when charindex(',', ELEM)>0 then charindex(',', ELEM)-1 end)) as ELEM,"
						+ " sum(P2CCOMPLACK) as counter1, sum(P2CREQUTOT) as counter2  " + 
						"from DC_E_CNAXE_MMESTAT_RAW  " + 
						"where UTC_DATETIME_ID='"+datestamp+"' and statistics_type <> 'CLUSTER' and OSS_ID = '" + ossID +
						"' group by ELEM, OSS_ID, DATETIME_ID ";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					float counter1 = result.getFloat("counter1");
					float counter2 = result.getFloat("counter2");
					
					BigDecimal KPI_Value = null;
					String breach = null;
					try{
						KPI_Value = new BigDecimal( 100*(counter1/counter2) );
						KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
						breach = calculateBreach("15", KPI_Value);
						mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
						log.log(Level.FINEST, "Counter values were : counter1="+ counter1 + ", counter2=" + counter2);
					}
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("ELEM"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "15");
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					




				}
			}else{
				log.warning("DC_E_CNAXE is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 15: ", e);
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
	
	public MeasurementFile KPI16(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 16");
			if(checkActiveTP(activeTPs, 16)){
				String sql = "select DATETIME_ID, oss_id, sn, left(elem, (case when charindex(',', ELEM)=0 then length(elem) when charindex(',', ELEM)>0 then charindex(',', ELEM)-1 end)) as ELEM,"
						+ " 100*avg(SRVCC2U_NRELREQSUCC/SRVCC2U_NRELREQTOT) as KPI_VALUE " + 
						"from DC_E_CNAXE_MSC_RAW  " + 
						"where UTC_DATETIME_ID='" + datestamp + "' and statistics_type <> 'CLUSTER' AND OSS_ID='" +ossID+"' "  + 
						"group by oss_id, sn, elem, DATETIME_ID";
				
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {	
					try{
						BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_VALUE") );
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("16", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("ELEM"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "16");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_CNAXE is not installed.");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 16: ", e);
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
	
	public MeasurementFile KPI17(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 17");
			if(checkActiveTP(activeTPs, 17)){
				
				String sql = "select DATETIME_ID, left(elem, (case when charindex(',', ELEM)=0 then length(elem) when charindex(',', ELEM)>0 then charindex(',', ELEM)-1 end)) as ELEM,"
						+ " sum(NBRMSCS2U_NS2UASUCC) as counter1, sum(NBRMSCS2U_NS2UATOT) as counter2  " + 
						"from DC_E_CNAXE_MSCOBJ_RAW  " + 
						"where UTC_DATETIME_ID='" + datestamp + "' and statistics_type <> 'CLUSTER' and OSS_ID='"+ossID + 
						"' group by ELEM, OSS_ID, DATETIME_ID";
				
				sql = Utils.getPartitions(repdb, sql);
				
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					float counter1 = result.getFloat("counter1");
					float counter2 = result.getFloat("counter2");
					
					BigDecimal KPI_Value = null;
					String breach = null;
					try{
						KPI_Value = new BigDecimal( 100*(counter1/counter2) );
						KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
						breach = calculateBreach("17", KPI_Value);
						mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
						log.log(Level.FINEST, "Counter values were : counter1="+ counter1 + ", counter2=" + counter2);
					}
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("ELEM"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "17");
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					




				}
			}else{
				log.warning("DC_E_CNAXE is not installed.");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 17: ", e);
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
	
	public MeasurementFile KPI18(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 18");
			if(checkActiveTP(activeTPs, 18)){
				String sql = "select DATETIME_ID, NE_ID, avg(sbgSipNetIncSessionSetupTime/sbgSipNetIncSessionSetupUserSuccess) as KPI_VALUE" +
						" from  DC_E_IMSSBG_SIP_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' GROUP BY NE_ID, DATETIME_ID";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
	
					try{
						BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_VALUE"));
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("18", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "18");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_IMSGW_SBG is not installed");
			}
			
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 18: ", e);
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
	
	public MeasurementFile KPI19(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 19");
			if(checkActiveTP(activeTPs, 19)){
				String sql = "select DATETIME_ID, NE_ID, avg(sbgSipNetOutSessionSetupTime/sbgSipNetOutSessionSetupUserSuccess) as KPI_VALUE" +
						" from  DC_E_IMSSBG_SIP_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' GROUP BY NE_ID, DATETIME_ID";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					
					try{
						BigDecimal KPI_Value = new BigDecimal(result.getFloat("KPI_VALUE"));
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("19", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "19");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_IMSGW_SBG is not installed");
			}
			
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 19: ", e);
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
	
	public MeasurementFile KPI20(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 20");
			if(checkActiveTP(activeTPs, 20)){
				String sql = "select DATETIME_ID, NE_ID, avg(sbgInitRegTime/sbgSipRegStatInitialAccCnt) as KPI_VALUE" +
						" from  DC_E_IMSSBG_PROXYREGISTRAR_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' GROUP BY NE_ID, DATETIME_ID";
				sql = Utils.getPartitions(repdb, sql);
				
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					
					try{
						BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_VALUE"));
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("20", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("DATETIME_ID", datestamp);
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "20");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_IMSGW_SBG is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 20: ", e);
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
	
	public MeasurementFile KPI21(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 21");
			if(checkActiveTP(activeTPs, 21)){
				/*String sql = "select DATETIME_ID, SGSN, VS_MM_SrvccCsOnlyToWAtt_E" +
						", VS_MM_SrvccCsAndPsToWAtt_E from  DC_E_SGSNMME_MOBILITY_MM_E_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";*/
				
				String sql = "select DATETIME_ID, SGSN, isnull(VS_MM_SrvccCsOnlyToWAtt_E + VS_MM_SrvccCsAndPsToWAtt_E, "
						+ "VS_MM_SrvccCsOnlyToWAtt_E, "
						+ "VS_MM_SrvccCsAndPsToWAtt_E) as KPI_Value "
						+ "from  DC_E_SGSNMME_MOBILITY_MM_E_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";
				
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					/*float counter1 = result.getFloat("VS_MM_SrvccCsOnlyToWAtt_E");
					float counter2 = result.getFloat("VS_MM_SrvccCsAndPsToWAtt_E");*/
	
					try{
						//BigDecimal KPI_Value = new BigDecimal( counter1+counter2 );
						
						BigDecimal KPI_Value = new BigDecimal(result.getFloat("KPI_Value"));
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("21", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("SGSN"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "21");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_SGSNMME is not installed.");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 21: ", e);
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
	
	public MeasurementFile KPI22(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 22");
			if(checkActiveTP(activeTPs, 22)){
				String sql = "select DATETIME_ID, NE_ID, sum(sbgSipRespondedIncSessions) as KPI_VALUE" +
						" from  DC_E_IMSSBG_SIP_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' GROUP BY NE_ID, DATETIME_ID";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					try{
						BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_VALUE") );
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("22", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "22");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_IMSGW_SBG is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 22: ", e);
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
	
	public MeasurementFile KPI23(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 23");
			if(checkActiveTP(activeTPs, 23)){
				String sql = "select DATETIME_ID, NE_ID, avg(sbgSgcPktsLostRatioGauge)/100 as KPI_VALUE" +
						" from  DC_E_IMSSBG_NETWORKQOS_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' GROUP BY NE_ID, DATETIME_ID";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					try{
						BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_VALUE"));
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("23", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "23");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_IMSGW_SBG is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 23: ", e);
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

	public MeasurementFile KPI25(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 25");
			if(checkActiveTP(activeTPs, 25)){
				String sql = "select DATETIME_ID, g3ManagedElement, MtasPriorityCallWpsEstablished" +
						", MtasPriorityCallWpsRequested from  DC_E_MTAS_MTASPRIORITYCALL_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "'";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					float counter1 = result.getFloat("MtasPriorityCallWpsEstablished");
					float counter2 = result.getFloat("MtasPriorityCallWpsRequested");
					
					String breach = null;
					BigDecimal KPI_Value = null;
					try{
						KPI_Value = new BigDecimal( 100*(counter1/counter2) );
						KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
						breach = calculateBreach("25", KPI_Value);
						mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
						log.log(Level.FINEST, "Counter values were : counter1="+ counter1 + ", counter2=" + counter2);
					}
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("g3ManagedElement"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "25");
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					




				}
			}else{
				log.warning("DC_E_MTAS is not installed.");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 25: ", e);
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
	
	public MeasurementFile KPI26(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 26");
			if(checkActiveTP(activeTPs, 26)){
				String sql = "select NE_ID, DATETIME_ID, 100*avg(sbgSgcIncPriorityCallSuccessSetups/sbgSgcIncPriorityCallAttempts) as KPI_VALUE" +
						" from  DC_E_IMSSBG_SIGNWCN_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' GROUP BY NE_ID, DATETIME_ID";
				
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					
					try{
						BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_VALUE") );
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("26", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "26");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_IMSGW_SBG is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 26: ", e);
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
	
	public MeasurementFile KPI27(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 27");
			if(checkActiveTP(activeTPs, 27)){
				String sql = "select NE_ID, DATETIME_ID, 100*avg(sbgSgcTotalEmergencyCallsSuccess/sbgSgcTotalEmergencyCallsSetups) as KPI_VALUE" +
						" from  DC_E_IMSSBG_SIGNWCN_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' GROUP BY NE_ID, DATETIME_ID";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {					
					
					try{
						BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_VALUE") );
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("27", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "27");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_IMSGW_SBG is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 27: ", e);
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
	
	public MeasurementFile KPI28(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 28");
			if(checkActiveTP(activeTPs, 28)){
				String sql = "select DATETIME_ID, NE_ID, sum(sbgSipEmerRegStatInitialAccCnt) as KPI_VALUE" +
						" from  DC_E_IMSSBG_PROXYREGISTRAR_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID + "' GROUP BY NE_ID, DATETIME_ID";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					try{
						BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_VALUE") );
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("28", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("NE_ID"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "28");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_IMSGW_SBG is not installed");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 28: ", e);
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

	
	
	public MeasurementFile KPI34(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 34");
			if(checkActiveTP(activeTPs, 34)){
				/*String sql = "select SGSN, DATETIME_ID, MOID, VS_SM_CreateDedicatedBearerQCIAtt_E " +
						", VS_SM_CreateDedicatedBearerQCISucc_E from  DC_E_SGSNMME_SESSION_SM_E_QCI_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID +
						"' and MOID like '%QosClassIdentifier=1'";*/
				
				String sql = "select SGSN, DATETIME_ID, MOID, "
						+ "100*(VS_SM_CreateDedicatedBearerQCISucc_E/VS_SM_CreateDedicatedBearerQCIAtt_E) as KPI_Value "
						+ "from  DC_E_SGSNMME_SESSION_SM_E_QCI_RAW  where UTC_DATETIME_ID='" + datestamp + "' and OSS_ID='" + ossID +
						"' and MOID like '%QosClassIdentifier=1'";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				while (result.next()) {
					/*float ctr1 = result.getFloat("VS_SM_CreateDedicatedBearerQCIAtt_E");
					float ctr2 = result.getFloat("VS_SM_CreateDedicatedBearerQCISucc_E");*/
					
					try{
						//BigDecimal KPI_Value = new BigDecimal( 100*(ctr2/ctr1) );
						BigDecimal KPI_Value = new BigDecimal(result.getFloat("KPI_Value"));
						String breach = null;
						if (!result.wasNull()){
				            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
				            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
				            breach = calculateBreach("34", KPI_Value);
						}
						
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", result.getString("SGSN"));
						nodeDetails.put("OSS_ID", ossID);
						
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "34");
						//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_SGSNMME is not installed.");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 34: ", e);
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
	
	public MeasurementFile KPI35(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running KPI 35");
			if(checkActiveTP(activeTPs, 35)){
				String sql = "select SGSN, DATETIME_ID, MOID, VS_SM_CreateDedicatedBearerQCIAtt_E " +
						" from  DC_E_SGSNMME_SESSION_SM_E_QCI_RAW  where UTC_DATETIME_ID='" + datestamp  + "' and OSS_ID='" + ossID + "'";
				sql = Utils.getPartitions(repdb, sql);
				result = dbconn.getConnection().createStatement().executeQuery(sql);
				
				HashMap<String, HashMap<String, String>> KPIs = new HashMap<String, HashMap<String, String>>();
				while (result.next()) {
					String ME = result.getString("SGSN");
					String moid = result.getString("MOID");
					String OSS_ID = ossID;
					String nodeID = ME + "::" + OSS_ID;
					
					float counter1 = 0;
					float counter2 = 0;
					int QCI = 0;
					try{
						QCI = Integer.parseInt(moid.split("QosClassIdentifier=")[1]);
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "QosClassIdentifier value is not an integer");
						continue;
					}
					float counterValue = result.getFloat("VS_SM_CreateDedicatedBearerQCIAtt_E");
					if (QCI == 1) {
						counter1 = counterValue;
						counter2 = counterValue;
					} else if (QCI <= 9){
						counter2 = counterValue;
					}
					
					if(KPIs.containsKey(nodeID)){
						HashMap<String, String> values = KPIs.get(nodeID);
						
						counter1 = counter1 + Float.parseFloat(values.get("counter1"));
						counter2 = counter2 + Float.parseFloat(values.get("counter2"));
						values.put("counter1", ""+counter1);
						values.put("counter2", ""+counter2);
						
						KPIs.put(nodeID, values);
						
					}else{
						HashMap<String, String> values = new HashMap<String, String>();
						values.put("SGSN", ME);
						values.put("MOID", moid);
						values.put("DATETIME_ID", result.getString("DATETIME_ID"));
						values.put("OSS_ID", ossID);
						values.put("counter1", ""+counter1);
						values.put("counter2", ""+counter2);
						KPIs.put(nodeID, values);
					}
				}
				
				
				for(String nodeID : KPIs.keySet()){
					HashMap<String, String> values = KPIs.get(nodeID);
					
					float counter1 = Float.parseFloat(values.get("counter1"));
					float counter2 = Float.parseFloat(values.get("counter2"));
					
					String breach = null;
					BigDecimal KPI_Value = null;
					try{
						KPI_Value = new BigDecimal( (100 * counter1) / (counter2) );
						KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
						breach = calculateBreach("35", KPI_Value);
						mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
						mFile.addData("BREACH_INDICATION", breach);
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
						log.log(Level.FINEST, "Counter values were : counter1="+ counter1 + ", counter2=" + counter2);
					}
						HashMap<String, String> nodeDetails = new HashMap<String, String>();
						nodeDetails.put("NODE_NAME", values.get("SGSN"));
						nodeDetails.put("OSS_ID", ossID);
		
						mFile.addData("NODE_ID", voltedim.findNode(dbconn, nodeDetails));
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", values.get("DATETIME_ID"));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("KPI_ID", "35");
						mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					




				}
			}else{
				log.warning("DC_E_SGSNMME is not installed.");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 35: ", e);
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
	
	public MeasurementFile QCI34(MeasurementFile mFile, RockFactory dbconn, String datestamp, List<String> activeTPs, String ossID){
		try{
			log.log(Level.FINE, "Running QCI 34");
			if(checkActiveTP(activeTPs, 34)){
				for(int index=1; index<=9; index++){
					String sql = "select DATETIME_ID, VS_SM_CreateDedicatedBearerQCIAtt_E " +
							", VS_SM_CreateDedicatedBearerQCISucc_E from  DC_E_SGSNMME_SESSION_SM_E_QCI_RAW  where UTC_DATETIME_ID='" + datestamp +  
							"' and MOID like '%QosClassIdentifier=" + index + "'";
					
					sql = Utils.getPartitions(repdb, sql);
					result = dbconn.getConnection().createStatement().executeQuery(sql);
					
					float ctr1 = 0;
					float ctr2 = 0;
					
					while (result.next()) {
					
						ctr1 = ctr1 + result.getFloat("VS_SM_CreateDedicatedBearerQCIAtt_E");
						ctr2 = ctr2 + result.getFloat("VS_SM_CreateDedicatedBearerQCISucc_E");
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
					}
					
					try{
						BigDecimal KPI_Value = new BigDecimal( 100*(ctr2/ctr1) );
						KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
						
						//mFile.addData("OSS_ID", ossID);
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("QCI_ID", String.valueOf(index));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("QCI_VALUE", KPI_Value.toPlainString());
						mFile.addData("MEASURE_TYPE", "Success");
						
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert QCI result value : "+ e.getLocalizedMessage());
					}
				}
			}else{
				log.warning("DC_E_SGSNMME is not installed.");
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute QCI 34: ", e);
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
	
	public void calculateERBSKPIs(MeasurementFile mFile, MeasurementFile qciFile, VolteERBS erbs, List<String> activeTPs, int kpi_id, String ossID){
		int scenario = 0;
		if(activeTPs.contains("DC_E_ERBS")){
			scenario = scenario + 1;
		}
		if(activeTPs.contains("DC_E_ERBSG2")){
			scenario = scenario + 2;
		}
		
		if(scenario > 0){
			
			if(erbs.getTopGroups()){
				log.log(Level.FINE, "ERBS TP installed, running ERBS KPI's");
				erbs.ERBS_Ranking_Sample_1(ossID);
				erbs.ERBS_Ranking_Sample_2(ossID);
			}
			switch(kpi_id){
			case 24 :
				erbs.ERBSKPI(mFile, "24", VolteERBSUtils.SQL_Skeleton_KPIA, VolteERBSUtils.SQL_Query_KPI_24, VolteERBSUtils.And_Where_ERBS, "dataTables", ossID);
				break;
			case 29 :
				erbs.ERBSKPI29(mFile, ossID);
				break;
			case 30 :
				erbs.ERBSKPI(mFile, "30", VolteERBSUtils.SQL_Skeleton_KPIA, VolteERBSUtils.SQL_Query_KPI_30, VolteERBSUtils.And_Where_ERBS, "VectorTables", ossID);
				break;
			case 31 :
				erbs.ERBSKPI(mFile, "31", VolteERBSUtils.SQL_Skeleton_KPI_31, VolteERBSUtils.SQL_Query_KPI_31, VolteERBSUtils.And_Where_ERBS, "VectorTables",ossID);
				break;
			case 32 :
				erbs.ERBSKPIALT(mFile, "32", VolteERBSUtils.SQL_Skeleton_KPIB, VolteERBSUtils.SQL_Query_KPI_32_NUMERATOR, VolteERBSUtils.SQL_Query_KPI_32_DENOMINATOR, VolteERBSUtils.Where_ERBS, "VectorTables",ossID);
				break;
			case 33 : 
				erbs.ERBSKPIALT(mFile, "33", VolteERBSUtils.SQL_Skeleton_KPIB, VolteERBSUtils.SQL_Query_KPI_33_NUMERATOR, VolteERBSUtils.SQL_Query_KPI_33_DENOMINATOR, VolteERBSUtils.Where_ERBS, "VectorTables",ossID);
				break;
			}			
		}
	}
	
	public String calculateBreach(String paramString, BigDecimal paramBigDecimal)
	  {
	    this.log.log(Level.FINEST, "Calculating Threshold breach for KPI " + paramString);
	    try{
	    	if (this.thresholds.containsKey(paramString))
		    {
		      String[] arrayOfString = ((String)this.thresholds.get(paramString)).split(":");
		      BigDecimal localBigDecimal = new BigDecimal(arrayOfString[0]);
		      if (arrayOfString[1].equalsIgnoreCase("1"))
		      {
		        if (paramBigDecimal.compareTo(localBigDecimal) == -1) {
		          return "1";
		        }
		      }
		      else if ((arrayOfString[1].equalsIgnoreCase("0")) && (paramBigDecimal.compareTo(localBigDecimal) == 1)) {
		        return "1";
		      }
		    }
		    else
		    {
		      this.log.log(Level.FINEST, "Threshold not found for KPI " + paramString);
		    }
		    return "0";
		  }catch(Exception e){
			  return "0";
		  }
	  }
}