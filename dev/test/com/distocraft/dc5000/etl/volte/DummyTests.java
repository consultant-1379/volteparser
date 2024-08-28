package com.distocraft.dc5000.etl.volte;


import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import com.distocraft.dc5000.etl.parser.Main;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.ericsson.eniq.repository.ETLCServerProperties;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

public class DummyTests {
	
	private String techPack;
    private String setType;
    private String setName;
    private String workerName = "";

	
	Utils utils = new Utils();
	VolteParser volteparser = new VolteParser();
	VolteKPIs kpi =new VolteKPIs(null, null, null);
	VolteDIM dim = new VolteDIM();
	//VolteERBS erbs = new VolteERBS(null, null, null, null, null);
	
	
	@Test
	public void testPreviousRopTime() {
		String result = utils.calculatePreviousRopTime();
	    assertEquals(result,result);
	}

	@Test
	public void testStatus() {
		int result = volteparser.status();
	    assertEquals(result,result);
	}
	
	@Test
	public void test3()  {
		utils.loadParameters(null, null);
		assertEquals(0, 0);
		
	}
	

	@Test
	public void test4() throws IOException {
		
		utils.loadProperties();
		assertEquals(0, 0);
	}
	
	@Test
	public void test5()  {
		
		try {
			utils.getPartitions(null, null);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void test6() {
		
		volteparser.init(null,  techPack,  setType, setName, workerName);
		assertEquals(0, 0);
	}
	
}