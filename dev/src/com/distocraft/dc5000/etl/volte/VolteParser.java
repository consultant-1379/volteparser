package com.distocraft.dc5000.etl.volte;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.parser.Main;
import com.distocraft.dc5000.etl.parser.MeasurementFile;
import com.distocraft.dc5000.etl.parser.Parser;
import com.distocraft.dc5000.etl.parser.SourceFile;
import com.distocraft.dc5000.etl.parser.TransformerCache;

public class VolteParser implements Parser{
	
	
	//private static final String JVM_TIMEZONE = new SimpleDateFormat("Z").format(new Date());
	
	private Logger log;
	private String techPack;
    private String setType;
    private String setName;
    private int status = 0;
    private Utils utils;
    private Main mainParserObject = null;
    private String workerName = "";
    private boolean completed = false;
    private int nodeidoffset = 0;
    private List<Integer> activeKPIs;
    private Map<String, String> parameters;
    private HashMap<String, String> thresholds;
    private List<String> activeTPs;
    private MeasurementFile mFile;
    private MeasurementFile qciFile;
    private ResultSet result;


	@Override
	public void init(final Main main, final String techPack, final String setType, final String setName, final String workerName){
        this.mainParserObject = main;
        this.techPack = techPack;
        this.setType = setType;
        this.setName = setName;
        this.status = 1;
        this.workerName = workerName;

        String logWorkerName = "";
        if (workerName.length() > 0) {
            logWorkerName = "." + workerName;
        }
        
        log = Logger.getLogger("etl." + techPack + "." + setType + "." + setName + ".parser.VOLTEParser" + logWorkerName);
    }
	
	
	@Override
	public void run() {
		try {

            this.status = 2;
            SourceFile sf = null;
            completed = false;
                  
            while ((sf = mainParserObject.nextSourceFile()) != null) {

                try {
                    mainParserObject.preParse(sf);
                    if(!completed){
                    	parse(sf, techPack, setType, setName);
                    }
                    mainParserObject.postParse(sf);
                } catch (final Exception e) {
                    mainParserObject.errorParse(e, sf);
                } finally {
                    mainParserObject.finallyParse(sf);
                }
            }
        } catch (final Exception e) {
            // Exception catched at top level. No good.
            log.log(Level.WARNING, "Worker parser failed to exception", e);
        } finally {
            this.status = 3;
        }
	}


	@Override
	public void parse(final SourceFile sf, final String techPack, final String setType, final String setName) throws Exception {
		RockFactory dwhdb = null;
		RockFactory repdb = null;
		
		utils = new Utils(log);
		utils.createDataFile(sf.getDir());
		utils.loadProperties();
		
		try{
			dwhdb = utils.getDBConn("dwhdb");
			repdb = utils.getDBConn("repdb");
			log.log(Level.FINEST, "Database connection established");
			
			TransformerCache.setCheckTransformations(false);
			String OSS_ID = techPack.split("-")[1];
			completed = true;
			getActiveKPIs(dwhdb);
			getParameters(dwhdb);
			getActiveTPs(repdb);
			
			nodeidoffset = Integer.parseInt(parameters.get("NODEIDOFFSET"));
			
			String utcdatetime = utils.calculatePreviousRopTime();
			
			if(techPack.startsWith("INTF_DC")){
				mFile = Main.createMeasurementFile(sf, "Volte KPI", techPack, setType, setName, this.workerName, log);
				qciFile = Main.createMeasurementFile(sf, "Volte Overview", techPack, setType, setName, this.workerName, log);
				try{
					if(!activeKPIs.isEmpty()){
						VolteKPIs DCkpi = new VolteKPIs(log, thresholds, repdb);
						VolteERBS erbs = new VolteERBS(log, dwhdb, repdb, utcdatetime, DCkpi);
						log.log(Level.INFO, "Calculating "+ activeKPIs.size() +" KPIs");
						for(int kpi_id : activeKPIs){
							switch(kpi_id){
							case 1 : mFile = DCkpi.KPI01(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 2 : mFile = DCkpi.KPI02(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 3 : mFile = DCkpi.KPI03(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 4 : mFile = DCkpi.KPI04(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 5 : mFile = DCkpi.KPI05(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 6 : mFile = DCkpi.KPI06(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 7 : mFile = DCkpi.KPI07(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 8 : mFile = DCkpi.KPI08(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 9 : mFile = DCkpi.KPI09(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 10 : mFile = DCkpi.KPI10(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 11 : mFile = DCkpi.KPI11(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 12 : mFile = DCkpi.KPI12(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 13 : mFile = DCkpi.KPI13(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 14 : mFile = DCkpi.KPI14(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 15 : mFile = DCkpi.KPI15(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 16 : mFile = DCkpi.KPI16(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 17 : mFile = DCkpi.KPI17(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 18 : mFile = DCkpi.KPI18(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 19 : mFile = DCkpi.KPI19(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 20 : mFile = DCkpi.KPI20(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 21 : mFile = DCkpi.KPI21(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 22 : mFile = DCkpi.KPI22(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 23 : mFile = DCkpi.KPI23(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 25 : mFile = DCkpi.KPI25(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 26 : mFile = DCkpi.KPI26(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 27 : mFile = DCkpi.KPI27(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 28 : mFile = DCkpi.KPI28(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 34 : mFile = DCkpi.KPI34(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 35 : mFile = DCkpi.KPI35(mFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
								break;
							case 24 : case 29 : case 30 : case 31 : case 32 : case 33 :
								DCkpi.calculateERBSKPIs(mFile, qciFile, erbs, activeTPs, kpi_id, OSS_ID);
								break;
							}
						}
						log.fine("Calculating success instances.");
						qciFile = DCkpi.QCI34(qciFile, dwhdb, utcdatetime, activeTPs, OSS_ID);
						if(activeTPs.contains("DC_E_ERBS")){
							log.fine("Calculating failure instances.");
							erbs.ERBSQCI(qciFile, OSS_ID);
						}
					}else{
						log.warning("No ACTIVE KPIs");
					}
				}catch(Exception e){
					log.log(Level.WARNING, "Exception", e);
				}finally{
					if(mFile != null)
						mFile.close();
					if(qciFile != null)
						qciFile.close();
				}
				
			}else{
				try{
					log.log(Level.INFO, "DIM LOADER");
					mFile = Main.createMeasurementFile(sf, "Node Dimension", techPack, setType, setName, this.workerName, log);		
					/*mFile.addData("filename", sf.getName());
				    mFile.addData("DC_SUSPECTFLAG", "");
				    mFile.addData("DIRNAME", sf.getDir());
				    mFile.addData("JVM_TIMEZONE", JVM_TIMEZONE);*/
				    mFile.addData("DATETIME_ID", utcdatetime);
					mFile.addData("DATE_ID", utcdatetime.split(" ")[0]);
					
					result = dwhdb.getConnection().createStatement().executeQuery("select max(NODE_ID) as max_id from DIM_E_VOLTE_NODE");
					int nodecount = 0;
					if(result.next()){
						nodecount = Integer.valueOf(result.getString("max_id"));
						
					}
					
					if(nodecount < nodeidoffset){
						nodecount = nodecount + nodeidoffset;
					}
					
					VolteDIM dim = new VolteDIM(nodecount, log);
					mFile = dim.getERBSTopology(mFile, dwhdb, OSS_ID);
					mFile = dim.getCNCommonTopology(mFile, dwhdb, "SBG", "'SBG', 'Isite', 'vInfra'", "IMS", "SBG", OSS_ID);
					mFile = dim.getCNCommonTopology(mFile, dwhdb, "MTAS", "", "IMS", "MTAS", OSS_ID);
					mFile = dim.getCNCommonTopology(mFile, dwhdb, "CSCF", "", "IMS", "CSCF", OSS_ID);
					mFile = dim.getCNCommonTopology(mFile, dwhdb, "SGSN", "", "EPC", "MME", OSS_ID);
					//mFile = dim.getCNCommonTopology(mFile, dwhdb, "GGSN", "", "EPC", "PGW", OSS_ID);
					
					mFile = dim.getCNTopology(mFile, dwhdb, "DIM_E_CN_AXE", "MSC", "Core", OSS_ID);
					mFile = dim.getGGSNTopology(mFile, dwhdb, OSS_ID);
					log.log(Level.INFO, ""+mFile.getRowCount());
					
				}catch(Exception e){
					log.log(Level.WARNING, "Exception", e);
				}finally{
					if(mFile != null)
						mFile.close();
					if(result != null)
						result.close();
				}
			}
		}catch(NumberFormatException e){
			log.severe("Parameters not defined.");
		}catch(Exception e){
			log.severe("Exception " +e);
		}finally{
			try{
				if(dwhdb!=null){
					if((dwhdb.getConnection()!=null ) && (!dwhdb.getConnection().isClosed())){
						dwhdb.getConnection().close();
						log.info("Dwhdb connection is closed "+dwhdb.getConnection().isClosed());
					}
				}
			}catch(Exception e){
				log.warning("Exception while closing the dwhdb connection "+e.getMessage());
			}
			try{
				if(repdb!=null){
					if((repdb.getConnection()!=null ) && (!repdb.getConnection().isClosed())){
						repdb.getConnection().close();
						log.info("repdb connection is closed "+repdb.getConnection().isClosed());
					}
				}
			}catch(Exception e){
				log.warning("Exception while closing the repdb connection "+e.getMessage());
			}
		}
	}
	
	
	private ResultSet executeQuery(RockFactory dbconn, String query){
		try{
			result = dbconn.getConnection().createStatement().executeQuery(query);
		}catch(Exception e){
			log.warning("Error in executing the query - " + query);
		}		
		return result;
		
	}
	
	private void getParameters(RockFactory dwhdb){
		parameters = new HashMap<>();
		try{
			result = executeQuery(dwhdb, "Select * from DIM_E_VOLTE_CONFIG");
			while(result.next()){
				parameters.put(result.getString("PARAMETER"), result.getString("VALUE"));
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Could not get parameters");
			
		}finally{
			try {
				result.close();
			} catch (SQLException e) {
				log.warning("Could not close result set for parameters. "+ e);
			}
		}
	}
	
	private void getActiveKPIs(RockFactory dwhdb){
		activeKPIs = new ArrayList<>();
		thresholds = new HashMap<>();
		log.log(Level.INFO, "Loading KPI Thresholds");
		try {
			result = executeQuery(dwhdb,"select KPI_ID, THRESHOLD, THRESHOLD_TYPE from DIM_E_VOLTE_KPI where STATUS = 'ACTIVE'");
			while (result.next()) {
				activeKPIs.add(result.getInt("KPI_ID"));
				thresholds.put(result.getString("KPI_ID"), result.getString("THRESHOLD") + ":" + result.getString("THRESHOLD_TYPE"));
			}
		} catch (SQLException e) {
			log.warning("Could not find active KPIs. " +e);
			
		} finally{
			try {
				result.close();
			} catch (SQLException e) {
				log.warning("Could not close result set for ACTIVE KPIs. "+ e);
			}
		}
	}
	
	private void getActiveTPs(RockFactory repdb){
		activeTPs = new ArrayList<>();
		
		try{			
			result = executeQuery(repdb,"Select TECHPACK_NAME from TPActivation where STATUS='ACTIVE'");
			while (result.next()) {
				activeTPs.add(result.getString("TECHPACK_NAME"));
			}
		}catch(Exception e){
			log.log(Level.WARNING, "Could not find active partitions", e);
		}finally{
			try {
				result.close();
			} catch (SQLException e) {
				log.warning("Could not close result set for active partitions. "+ e);
			}
		}
		
	}

	@Override
    public int status() {
        return status;
    }

}
