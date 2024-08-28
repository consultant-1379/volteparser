package com.distocraft.dc5000.etl.volte;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.etl.parser.MeasurementFile;

import ssc.rockfactory.RockFactory;

public class VolteERBS {

	private Logger log;
	private RockFactory dwhdb;
	private RockFactory dwhrep;
	private HashMap<String, ArrayList<String>> topGroups;
	private HashMap<String, ArrayList<String>> partitions;
	private String datestamp;
	private VolteKPIs volteKpi;
	private VolteDIM volteDim;
	private ResultSet result;
	
	
	public VolteERBS(Logger log, RockFactory dwhdb, RockFactory dwhrep, String datestamp, VolteKPIs volteKpi){
		this.log = log;
		this.dwhdb = dwhdb;
		this.dwhrep = dwhrep;
		this.datestamp = datestamp;
		this.volteKpi = volteKpi;
		this.volteDim = new VolteDIM();
		this.topGroups = new HashMap<String, ArrayList<String>>();
		this.partitions = new HashMap<String, ArrayList<String>>();
		getTablePartitionNames();
	}
	
	public void ERBS_Ranking_Sample_1(String ossID){
		log.log(Level.FINE, "Running ranking for ID 1");
		String Full_SQL_Query = "";
		try{
			ArrayList<String> PartitionNames = partitions.get("dataTables");
			String Factor1 = populateSQL(VolteERBSUtils.SQL_Query_KPI_29_FACTOR1, PartitionNames, "", "");
			String Factor2 = populateSQL(VolteERBSUtils.SQL_Query_KPI_29_FACTOR2, PartitionNames, "", "");
			PartitionNames = partitions.get("VectorTables");
			String Factor3 = populateSQL(VolteERBSUtils.SQL_Query_KPI_29_FACTOR3, PartitionNames, "", "");
			
			Full_SQL_Query = VolteERBSUtils.SQL_Skeleton_KPI_29.replace("$FACTOR_1", Factor1);
			Full_SQL_Query = Full_SQL_Query.replace("$FACTOR_2", Factor2);
			Full_SQL_Query = Full_SQL_Query.replace("$FACTOR_3", Factor3);
			
			Full_SQL_Query = VolteERBSUtils.SQL_Skeleton_Ranking.replace("$Query", Full_SQL_Query);
			Full_SQL_Query = Full_SQL_Query.replace("$datestamp", datestamp);
			
			Full_SQL_Query = Full_SQL_Query.replace("AND OSS_ID = '$OSS'", "");
			
//			log.log(Level.FINEST, "Full Query: \n" + Full_SQL_Query);
			result = dwhdb.getConnection().createStatement().executeQuery(Full_SQL_Query);

			ArrayList<String> ERBSList = new ArrayList<String>();
			while (result.next()) {
				String ERBS = result.getString("ERBS");
				if(!ERBS.equals("")){
					ERBSList.add(ERBS);
				}
			}
			topGroups.put("1", ERBSList);

		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute ranking for ID 1", e);
			log.log(Level.FINEST, "Unable to execute query : \n" + Full_SQL_Query);
		}finally{
			try {
				if(result != null)
					result.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void ERBS_Ranking_Sample_2(String ossID){
		log.log(Level.FINE, "Running ranking for ID 2");
		String Full_SQL_Query = "";
		try{
			ArrayList<String> PartitionNames = partitions.get("VectorTables");
			Full_SQL_Query = populateSQL(VolteERBSUtils.SQL_Query_KPI_31, PartitionNames, "", "");
			
			Full_SQL_Query = VolteERBSUtils.SQL_Skeleton_KPI_31.replace("$Query", Full_SQL_Query);
			Full_SQL_Query = VolteERBSUtils.SQL_Skeleton_Ranking.replace("$Query", Full_SQL_Query);
			Full_SQL_Query = Full_SQL_Query.replace("$datestamp", datestamp);
			
			Full_SQL_Query = Full_SQL_Query.replace("and OSS_ID = '$OSS'", "");
			
//			log.log(Level.FINEST, "Full Query: \n" + Full_SQL_Query);
			result = dwhdb.getConnection().createStatement().executeQuery(Full_SQL_Query);

			ArrayList<String> ERBSList = new ArrayList<String>();
			while (result.next()) {
				String ERBS = result.getString("ERBS");
				if(!ERBS.equals("")){
					ERBSList.add(ERBS);
				}
			}

			topGroups.put("2", ERBSList);

		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute ranking for ID 2", e);
			log.log(Level.FINEST, "Unable to execute query : \n" + Full_SQL_Query);
		}finally{
			try {
				if(result != null)
					result.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public MeasurementFile ERBSKPI29(MeasurementFile mFile, String ossID){
		log.log(Level.FINE, "Running KPI 29");
		String Full_SQL_Query = "";
		try{
			
			String ERBSList = "";
			for(String sampleid : topGroups.keySet()){
				String ERBS = topGroups.get(sampleid).toString().replace("[", "'").replace("]", "'").replace(", ", "','");
				if(!ERBS.equals("")){
					if(!ERBSList.equals("")){
						ERBSList = ERBSList + " , ";
					}
					ERBSList = ERBSList + ERBS;
				}
				
			}
			
			ArrayList<String> PartitionNames = partitions.get("dataTables");
			String Factor1 = populateSQL(VolteERBSUtils.SQL_Query_KPI_29_FACTOR1, PartitionNames, VolteERBSUtils.Where_ERBS, ERBSList);
			String Factor2 = populateSQL(VolteERBSUtils.SQL_Query_KPI_29_FACTOR2, PartitionNames, VolteERBSUtils.Where_ERBS, ERBSList);
			PartitionNames = partitions.get("VectorTables");
			String Factor3 = populateSQL(VolteERBSUtils.SQL_Query_KPI_29_FACTOR3, PartitionNames, VolteERBSUtils.And_Where_ERBS, ERBSList);
			
			Full_SQL_Query = VolteERBSUtils.SQL_Skeleton_KPI_29.replace("$FACTOR_1", Factor1);
			Full_SQL_Query = Full_SQL_Query.replace("$FACTOR_2", Factor2);
			Full_SQL_Query = Full_SQL_Query.replace("$FACTOR_3", Factor3);
			Full_SQL_Query = Full_SQL_Query.replace("$datestamp", datestamp);
			
			Full_SQL_Query = Full_SQL_Query.replace("$OSS", ossID);
			
//			log.log(Level.FINEST, "Full Query: \n" + Full_SQL_Query);
			
			if(!ERBSList.equals("")){
				result = dwhdb.getConnection().createStatement().executeQuery(Full_SQL_Query);
				
				while (result.next()) { 
					for(String sampleid : topGroups.keySet()){
						if(topGroups.get(sampleid).contains(result.getString("ERBS"))){
							ERBSPopulateMeasFile(result, sampleid, "29", mFile, dwhdb, datestamp, ossID);
						}
					}
				}
			
			}
			
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI 29", e);
			log.log(Level.FINEST, "Unable to execute query : \n" + Full_SQL_Query);
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
	
	public MeasurementFile ERBSKPI(MeasurementFile mFile, String KPINumber, String skeleton, String Query, String ERBSClause, String tableType, String ossID){
		log.log(Level.FINE, "Running KPI " + KPINumber);
		String Full_SQL_Query = "";
		try{
			String ERBSList = "";
			for(String sampleid : topGroups.keySet()){
				String ERBS = topGroups.get(sampleid).toString().replace("[", "'").replace("]", "'").replace(", ", "','");
				if(!ERBS.equals("")){
					if(!ERBSList.equals("")){
						ERBSList = ERBSList + " , ";
					}
					ERBSList = ERBSList + ERBS;
				}
				
			}
			
			ArrayList<String> PartitionNames = partitions.get(tableType);
			Full_SQL_Query = populateSQL(Query, PartitionNames, ERBSClause, ERBSList);
			Full_SQL_Query = skeleton.replace("$Query", Full_SQL_Query);
			Full_SQL_Query = Full_SQL_Query.replace("$datestamp", datestamp);
			
			Full_SQL_Query = Full_SQL_Query.replace("$OSS", ossID);
			
//			log.log(Level.FINEST, "Full Query: \n" + Full_SQL_Query);
			
			if(!ERBSList.equals("")){
				result = dwhdb.getConnection().createStatement().executeQuery(Full_SQL_Query);
				while (result.next()) { 
					for(String sampleid : topGroups.keySet()){
						if(topGroups.get(sampleid).contains(result.getString("ERBS"))){
							ERBSPopulateMeasFile(result, sampleid, KPINumber, mFile, dwhdb, datestamp, ossID);
						}
					}
				}
			}
			
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI " + KPINumber, e);
			log.log(Level.FINEST, "Unable to execute query : \n" + Full_SQL_Query);
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
	
	public MeasurementFile ERBSKPIALT(MeasurementFile mFile, String KPINumber, String skeleton, String NUMQuery, String DENQuery, String ERBSClause, String tableType, String ossID){
		log.log(Level.FINE, "Running KPI " + KPINumber);
		String Full_SQL_Query = "";
		try{
			String ERBSList = "";
			for(String sampleid : topGroups.keySet()){
				String ERBS = topGroups.get(sampleid).toString().replace("[", "'").replace("]", "'").replace(", ", "','");
				if(!ERBS.equals("")){
					if(!ERBSList.equals("")){
						ERBSList = ERBSList + " , ";
					}
					ERBSList = ERBSList + ERBS;
				}
				
			}
			
			ArrayList<String> PartitionNames = partitions.get(tableType);
			String numerator = populateSQL(NUMQuery, PartitionNames, ERBSClause, ERBSList);
			String denominator = populateSQL(DENQuery, PartitionNames, ERBSClause, ERBSList);
			Full_SQL_Query = skeleton.replace("$NUMERATOR", numerator);
			Full_SQL_Query = Full_SQL_Query.replace("$DENOMINATOR", denominator);
			Full_SQL_Query = Full_SQL_Query.replace("$datestamp", datestamp);
			
			Full_SQL_Query = Full_SQL_Query.replace("$OSS", ossID);
			
//			log.log(Level.FINEST, "Full Query: \n" + Full_SQL_Query);
			
			if(!ERBSList.equals("")){
				result = dwhdb.getConnection().createStatement().executeQuery(Full_SQL_Query);
				while (result.next()) { 
					for(String sampleid : topGroups.keySet()){
						if(topGroups.get(sampleid).contains(result.getString("ERBS"))){
							ERBSPopulateMeasFile(result, sampleid, KPINumber, mFile, dwhdb, datestamp, ossID);
						}
					}
				}
			}
			
		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute KPI " + KPINumber, e);
			log.log(Level.FINEST, "Unable to execute query : \n" + Full_SQL_Query);
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
	
	private void ERBSPopulateMeasFile(ResultSet result, String sampleid, String KPINumber, MeasurementFile mFile, RockFactory dbconn, String datestamp, String ossID) throws Exception{
		try{
			BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_VALUE") );
			String breach = null;
			if (!result.wasNull()){
	            KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
	            mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
	            breach = volteKpi.calculateBreach(KPINumber, KPI_Value);
			}
			
			HashMap<String, String> nodeDetails = new HashMap<String, String>();
			nodeDetails.put("NODE_NAME", result.getString("ERBS"));
			nodeDetails.put("OSS_ID", ossID);

			mFile.addData("NODE_ID", volteDim.findNode(dbconn, nodeDetails));
			mFile.addData("UTC_DATETIME_ID", datestamp);
			mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
			mFile.addData("TIMELEVEL", "15MIN");
			mFile.addData("PERIOD_DURATION", "15");
			mFile.addData("DATE_ID", datestamp.split(" ")[0]);
			mFile.addData("KPI_ID", KPINumber);
			mFile.addData("RAN_SAMPLE_ID", sampleid);
			//mFile.addData("KPI_VALUE", KPI_Value.toPlainString());
			mFile.addData("BREACH_INDICATION", breach);
			mFile.addData("OSS_ID", ossID);
			mFile.saveData();
		}catch(NumberFormatException e){
			log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
		}
	}
	
	public MeasurementFile ERBSQCI(MeasurementFile mFile, String ossID){
		try{
			ArrayList<String> PartitionNames = partitions.get("VectorTables");
			String Full_SQL_Query = populateSQL(VolteERBSUtils.SQL_Query_KPI_31, PartitionNames, "", "");
			Full_SQL_Query = VolteERBSUtils.SQL_Skeleton_KPI_31.replace("$Query", Full_SQL_Query);
			Full_SQL_Query = Full_SQL_Query.replace("$datestamp", datestamp);
			//Full_SQL_Query = Full_SQL_Query.replace("$OSS", ossID);
			Full_SQL_Query = Full_SQL_Query.replace("and OSS_ID = '$OSS'", "");
			Full_SQL_Query = Full_SQL_Query.replace("ERBS,","");
			Full_SQL_Query = Full_SQL_Query.replace("OSS_ID,","");
			Full_SQL_Query = Full_SQL_Query.replace(", OSS_ID","");
			
			
			for(int index=1; index<=9; index++){
				String QCI_SQL_Query = Full_SQL_Query.replace("DCVECTOR_INDEX=1", "DCVECTOR_INDEX="+index);
				result = dwhdb.getConnection().createStatement().executeQuery(QCI_SQL_Query);
				while (result.next()) {
					try{
						BigDecimal KPI_Value = new BigDecimal( result.getFloat("KPI_VALUE") );
						KPI_Value = KPI_Value.setScale(5, BigDecimal.ROUND_HALF_DOWN);
						
						mFile.addData("UTC_DATETIME_ID", datestamp);
						mFile.addData("DATETIME_ID", result.getString("DATETIME_ID"));
						mFile.addData("DATE_ID", datestamp.split(" ")[0]);
						mFile.addData("QCI_ID", ""+(index));
						mFile.addData("TIMELEVEL", "15MIN");
						mFile.addData("PERIOD_DURATION", "15");
						mFile.addData("QCI_VALUE", KPI_Value.toPlainString());
						mFile.addData("MEASURE_TYPE", "Failure");
						//mFile.addData("OSS_ID", ossID);
						mFile.saveData();
					}catch(NumberFormatException e){
						log.log(Level.WARNING, "Unable to convert KPI result value : "+ e.getLocalizedMessage());
					}
				}
				QCI_SQL_Query = Full_SQL_Query;
			}

		}catch(Exception e){
			log.log(Level.WARNING, "Unable to execute Failure QCI: ", e);
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
	
		
	private String populateSQL(String SQL_Query, ArrayList<String> tablenames, String ERBSClause, String ERBSList){
		String Full_SQL_Query = "";
		
		//populate the query for each active TP table partition
		for(String tablename : tablenames){
			if(!Full_SQL_Query.equals("")){
				Full_SQL_Query = Full_SQL_Query + " union ";
			}

			Full_SQL_Query = Full_SQL_Query + SQL_Query;
			Full_SQL_Query = Full_SQL_Query.replace("$tablename", tablename);
			if(tablename.contains("EUTRANCELLFDD")){
				Full_SQL_Query = Full_SQL_Query.replace("$cell", "EUTRANCELLFDD");
			}else if (tablename.contains("EUTRANCELLTDD")){
				Full_SQL_Query = Full_SQL_Query.replace("$cell", "EUTRANCELLTDD");
			}
			
			Full_SQL_Query = Full_SQL_Query.replace("$ERBSCLAUSE", ERBSClause);
			Full_SQL_Query = Full_SQL_Query.replace("$ERBSLIST", ERBSList);
			
		}
		Full_SQL_Query = Full_SQL_Query.replace("$datestamp", datestamp);
		
		return Full_SQL_Query;
	}

	private void getTablePartitionNames(){
		try{
			String basequery = VolteERBSUtils.Partition_Query.replace("$datestamp", datestamp);
			String query = basequery.replace("$TABLENAMES", VolteERBSUtils.ERBS_Data_Tables);
			
			//Get the active partitions for the data tables
			result = dwhrep.getConnection().createStatement().executeQuery(query);
			ArrayList<String> tablename = new ArrayList<String>();
			while (result.next()) {
				tablename.add(result.getString("tablename"));
			}
			partitions.put("dataTables", tablename);
			
			
			query = basequery.replace("$TABLENAMES", VolteERBSUtils.ERBS_Vector_Data_Tables);
			
			//Get the active partitions for the data tables
			result = dwhrep.getConnection().createStatement().executeQuery(query);
			tablename = new ArrayList<String>();
			while (result.next()) {
				tablename.add(result.getString("tablename"));
			}
			partitions.put("VectorTables", tablename);
			
			
		}catch(Exception e){
			log.log(Level.WARNING, "Worker parser failed to exception", e);
		}finally{
			try {
				if(result != null)
					result.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean getTopGroups() {
		return topGroups.isEmpty();
	}
	
	
}
