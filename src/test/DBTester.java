//package test;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//import java.io.File;
//
//import org.junit.jupiter.api.Test;
//
//import hw5.DB;
//
//class DBTester {
//	
//	/**
//	 * Things to consider testing:
//	 * 
//	 * Properly creates directory for new DB (done)
//	 * Properly accesses existing directory for existing DB
//	 * Properly accesses collection
//	 * Properly drops a database
//	 * Special character handling?
//	 */
//	
//	@Test
//	public void testCreateDB() {
//		DB hw5 = new DB("hw5"); //call method
//		assertTrue(new File("testfiles/hw5").exists()); //verify results
//	}
//
//}
package test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

import org.junit.jupiter.api.Test;

import mongo.DB;

class DBTester {

	/*
	 * Things to consider testing:
	 * 
	 * Properly returns collections
	 * Properly creates new collection
	 * Properly creates new DB (done)
	 * Properly drops db
	 */
	@Test
	public void testCreate() {
		DB db = new DB("testDB1");
		assertTrue(new File("testfiles/testDB1").exists());
		new File("testfiles/testDB1").delete();
	}
	
	@Test
	public void testCreateMulti() {
		DB db1 = new DB("TestDB1");
		assertTrue(new File("testfiles/TestDB1").exists());
		new File("testfiles/TestDB1").delete();
		DB db2 = new DB("TestDB2");
		assertTrue(new File("testfiles/TestDB2").exists());
		DB db3 = new DB("TestDB3");
		assertTrue(new File("testfiles/TestDB3").exists());
		new File("testfiles/TestDB2").delete();
		new File("testfiles/TestDB3").delete();
	}
	
	@Test
	public void testDrop() {
		DB db = new DB("testDelete");
		assertTrue(new File("testfiles/testDelete").exists());
		db.dropDatabase();
		assertTrue(!new File("testfiles/testDelete").exists());
	}

}