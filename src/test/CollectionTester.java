package test;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import mongo.DB;
import mongo.DBCollection;
import mongo.Document;

class CollectionTester {

	/*
	 * Things to be tested:
	 * 
	 * Document access (done?) Document insert/update/delete
	 */
	public static void print(Object ob) {
//		System.out.println("**********");
//		System.out.println(ob);
//		System.out.println("**********");
	}
//	@Test
//	public void testGetDocument1() {
//		DB db = new DB("data");
//		DBCollection dbc = db.getCollection("test");
//		JsonObject JO = dbc.getDocument(0);
//		assertTrue(JO.getAsJsonPrimitive("key").getAsString().equals("value"));
//	}
//
//	@Test
//	public void testGetDocument2() {
//		DB db = new DB("data");
//		DBCollection test = db.getCollection("test");
//		JsonObject object = test.getDocument(1);
//		assertTrue(object.getAsJsonObject("embedded").isJsonObject());
//		assertTrue(!object.getAsJsonObject("embedded").isJsonPrimitive());
//
//		JsonObject embedded = object.getAsJsonObject("embedded");
//		assertTrue(embedded.getAsJsonPrimitive("key2").getAsString().equals("value2"));
//
//	}

	@Test
	public void testGetDocument() {
		DB db = new DB("data");
		DBCollection dbCollection = db.getCollection("test");		
		JsonObject JO = dbCollection.getDocument(2);
		assertTrue(JO.get("array").isJsonArray());
		assertTrue(JO.getAsJsonArray("array").size() == 3);		
		JsonArray array = JO.getAsJsonArray("array");
		assertTrue(array.get(0).getAsString().equals("one"));
		assertTrue(array.get(1).getAsString().equals("two"));
		assertTrue(array.get(2).getAsString().equals("three"));
	}

	@Test
	public void testDrop() {
		DB db = new DB("drop");
		DBCollection test = db.getCollection("test1");
		File file = new File(test.getPath());
		print(file.getPath());
		print(file.getAbsolutePath());
		assertTrue(file.exists());
		test.drop();
		assertTrue(!file.exists());
	}

	@Test
	public void testSingleInsert() {
		DB db = new DB("insert");
		DBCollection test = db.getCollection("test1");
		String json = "{ name: \"sue\", age: 1, badges: [\"blue\"], birth: { month: 8, day: 12, week: \"Tue\" } }";
		test.insert(Document.parse(json));
		assertTrue(test.getName().equals("test1"));
		assertTrue(test.count() == 1);
		JsonObject object = test.getDocument(0);
		assertTrue(object.getAsJsonPrimitive("name").getAsString().equals("sue"));
		assertTrue(object.getAsJsonPrimitive("age").getAsInt() == 1);
		assertTrue(object.getAsJsonArray("badges").isJsonArray());
		JsonArray array = object.getAsJsonArray("badges");
		assertTrue(array.size() == 1);
		assertTrue(array.get(0).getAsString().equals("blue"));
		assertTrue(object.getAsJsonObject("birth").isJsonObject());
		JsonObject birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("month").getAsInt() == 8);
		assertTrue(birth.getAsJsonPrimitive("day").getAsDouble() == 12);
		assertTrue(birth.getAsJsonPrimitive("week").getAsString().equals("Tue"));
		db.dropDatabase();

	}

	@Test
	public void testMultileInsert() {
		DB db = new DB("insert");
		DBCollection test = db.getCollection("test2");
		String json1 = "{ name: \"abc\", age: 25, badges: [\"white\", \"red\"], birth: { month: 2, day: 21, week: \"Tue\" } }";
		String json2 = "{ name: \"bob\", age: 30, badges: [\"gray\"], birth: { month: 5, day: 13, week: \"Tue\" } }";
		String json3 = "{ name: \"alice\", age: 25, badges: [\"gel\", \"blue\"], birth: { month: 1, day: 7, week: \"Tue\" } }";
		test.insert(Document.parse(json1), Document.parse(json2), Document.parse(json3));
		assertTrue(test.getName().equals("test2"));
		assertTrue(test.count() == 3);
		JsonObject object = test.getDocument(0);
		assertTrue(object.getAsJsonPrimitive("name").getAsString().equals("abc"));
		assertTrue(object.getAsJsonPrimitive("age").getAsInt() == 25);
		assertTrue(object.getAsJsonArray("badges").isJsonArray());
		JsonArray array = object.getAsJsonArray("badges");
		assertTrue(array.size() == 2);
		assertTrue(array.get(0).getAsString().equals("white"));
		assertTrue(array.get(1).getAsString().equals("red"));
		assertTrue(object.getAsJsonObject("birth").isJsonObject());
		JsonObject birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("month").getAsInt() == 2);
		assertTrue(birth.getAsJsonPrimitive("day").getAsInt() == 21);
		assertTrue(birth.getAsJsonPrimitive("week").getAsString().equals("Tue"));
		object = test.getDocument(1);
		assertTrue(object.getAsJsonPrimitive("name").getAsString().equals("bob"));
		assertTrue(object.getAsJsonPrimitive("age").getAsInt() == 30);
		assertTrue(object.getAsJsonArray("badges").isJsonArray());
		array = object.getAsJsonArray("badges");
		assertTrue(array.size() == 1);
		assertTrue(array.get(0).getAsString().equals("gray"));
		assertTrue(object.getAsJsonObject("birth").isJsonObject());
		birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("month").getAsDouble() == 5);
		assertTrue(birth.getAsJsonPrimitive("day").getAsDouble() == 13);
		assertTrue(birth.getAsJsonPrimitive("week").getAsString().equals("Tue"));
		object = test.getDocument(2);
		assertTrue(object.getAsJsonPrimitive("name").getAsString().equals("alice"));
		assertTrue(object.getAsJsonPrimitive("age").getAsInt() == 25);
		assertTrue(object.getAsJsonArray("badges").isJsonArray());
		array = object.getAsJsonArray("badges");
		assertTrue(array.size() == 2);
		assertTrue(array.get(0).getAsString().equals("gel"));
		assertTrue(array.get(1).getAsString().equals("blue"));
		assertTrue(object.getAsJsonObject("birth").isJsonObject());
		birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("month").getAsDouble() == 1);
		assertTrue(birth.getAsJsonPrimitive("day").getAsDouble() == 7);
		assertTrue(birth.getAsJsonPrimitive("week").getAsString().equals("Tue"));
		db.dropDatabase();
	}
	@Test
	public void testupdate() {
		// TODO Auto-generated method stub
		DB db = new DB("update");
		DBCollection test = db.getCollection("test1");
		String json = "{ name: \"sue\", age: 19, birth: { month: 9, day: 8, week: \"Wed\" }, blood: \"O\" }";
		test.insert(Document.parse(json));
		String query = "{ name: \"sue\" }";
		String update = "{$set:{ \"birth.week\": \"Tue\", blood: \"B\"},$currentData:{lastModified:true} }";
		test.update(Document.parse(query), Document.parse(update), false);
//		print(test.getDocPool());
//		print(test.getDocPool().get(0));
		assertTrue(test.getName().equals("test1"));
		assertTrue(test.count() == 1);
		JsonObject object = test.getDocument(0);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("B"));
		JsonObject birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("week").getAsString().equals("Tue"));
		db.dropDatabase();
	}

	@Test
	public void testUpdate1() {
		DB db = new DB("update");
		DBCollection test = db.getCollection("test1");
		String json = "{ name: \"sue\", age: 19, birth: { month: 9, day: 8, week: \"Wed\" }, blood: \"O\" }";		
		test.insert(Document.parse(json));
		String query = "{ name: \"sue\" }";
		String update = "{ $set:{\"birth.week\": \"Tue\", blood: \"B\" },$currentData:{lastModified:true} }";
		test.update(Document.parse(query), Document.parse(update), false);
//		print(test.getDocPool());
//		print(test.getDocPool().get(0));
		assertTrue(test.getName().equals("test1"));
		assertTrue(test.count() == 1);
		JsonObject object = test.getDocument(0);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("B"));
		JsonObject birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("week").getAsString().equals("Tue"));
		db.dropDatabase();

	}

	@Test
	public void testUpdate2() {
		DB db = new DB("update");
		DBCollection test = db.getCollection("test2");
		String json = "{ name: \"sue\", age: 19, birth: { month: 9, day: 8, week: \"Wed\" }, blood: \"O\" }";
		test.insert(Document.parse(json));
		String query = "{ name: \"other\" }";
		String update = "{$set:{ \"birth.week\": \"Tue\", blood: \"P\" },$currentData:{lastModified:true} }";
		test.update(Document.parse(query), Document.parse(update), false);
		assertTrue(test.getName().equals("test2"));
		assertTrue(test.count() == 1);
		JsonObject object = test.getDocument(0);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("O"));
		JsonObject birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("week").getAsString().equals("Wed"));
		db.dropDatabase();

	}

	@Test
	public void testUpdate3() {
		DB db = new DB("update");
		DBCollection test = db.getCollection("test3");
		String json1 = "{ name: \"sue\", age: 19, birth: { month: 9, day: 8, week: \"Wed\" }, blood: \"A\" }";
		String json2 = "{ name: \"abc\", age: 25, birth: { month: 2, day: 21, week: \"Tue\" }, blood: \"A\" }";
		String json3 = "{ name: \"bob\", age: 85, birth: { month: 5, day: 13, week: \"Tue\" }, blood: \"A\" }";
		String json4 = "{ name: \"alice\", age: 25, birth: { month: 1, day: 7, week: \"Wed\" }, blood: \"B\" }";
		test.insert(Document.parse(json1), Document.parse(json2), Document.parse(json3), Document.parse(json4));
		assertTrue(test.count() == 4);
		String query = "{\"birth.week\": \"Tue\"}";
		String update = "{$set:{\"birth.day\": 15, \"blood\": \"O\"},$currentData:{lastModified:true} }";
		test.update(Document.parse(query), Document.parse(update), true);
		assertTrue(test.getName().equals("test3"));
		assertTrue(test.count() == 4);
		JsonObject object = test.getDocument(0);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("A"));
		JsonObject birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("week").getAsString().equals("Wed"));
		assertTrue(birth.getAsJsonPrimitive("day").getAsDouble() == 8);
		object = test.getDocument(1);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("O"));
		birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("week").getAsString().equals("Tue"));
		assertTrue(birth.getAsJsonPrimitive("day").getAsInt() == 15);
		object = test.getDocument(2);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("O"));
		birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("week").getAsString().equals("Tue"));
		assertTrue(birth.getAsJsonPrimitive("day").getAsInt() == 15);
		object = test.getDocument(3);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("B"));
		birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("week").getAsString().equals("Wed"));
		assertTrue(birth.getAsJsonPrimitive("day").getAsDouble() == 7);
		db.dropDatabase();

	}

	
	@Test
	public void testRemoveSingle() {
		DB db = new DB("remove");
		DBCollection test = db.getCollection("test1");
		String json1 = "{ name: \"sue\", age: 25, birth: { month: 2, day: 21, week: \"Tue\" }, blood: \"A\" }";
		String json2 = "{ name: \"bob\", age: 50, birth: { month: 6, day: 11, week: \"Wed\" }, blood: \"B\" }";
		String json3 = "{ name: \"ahn\", age: 100, birth: { month: 6, day: 11, week: \"Wed\" }, blood: \"O\" }";
		String json4 = "{ name: \"cat\", age: 75, birth: { month: 7, day: 30, week: \"Tue\" }, blood: \"O\" }";
		String json5 = "{ name: \"tom\", age: 45, birth: { month: 10, day: 18, week: \"Tue\" }, blood: \"A\" }";
		test.insert(Document.parse(json1), Document.parse(json2), Document.parse(json3), Document.parse(json4),Document.parse(json5));
		assertTrue(test.count() == 5);
		String query = "{ blood : \"A\" }";
		test.remove(Document.parse(query), false);
		assertTrue(test.count() == 4);
		JsonObject object = test.getDocument(0);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("B"));
		object = test.getDocument(1);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("O"));
		object = test.getDocument(2);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("O"));
		object = test.getDocument(3);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("A"));
		db.dropDatabase();
	}

	@Test
	public void testRemoveMulti() {
		DB db = new DB("remove");
		DBCollection test = db.getCollection("test2");
		String json1 = "{ name: \"sue\", age: 25, birth: { month: 2, day: 21, week: \"Tue\" }, blood: \"A\" }";
		String json2 = "{ name: \"bob\", age: 50, birth: { month: 6, day: 11, week: \"Wed\" }, blood: \"B\" }";
		String json3 = "{ name: \"ahn\", age: 100, birth: { month: 6, day: 11, week: \"Wed\" }, blood: \"O\" }";
		String json4 = "{ name: \"cat\", age: 75, birth: { month: 7, day: 30, week: \"Tue\" }, blood: \"O\" }";
		String json5 = "{ name: \"tom\", age: 45, birth: { month: 10, day: 18, week: \"Tue\" }, blood: \"A\" }";
		test.insert(Document.parse(json1), Document.parse(json2), Document.parse(json3), Document.parse(json4),Document.parse(json5));
		assertTrue(test.count() == 5);
		String query = "{ blood : \"A\" }";
		test.remove(Document.parse(query), true);
		assertTrue(test.count() == 3);
		JsonObject object = test.getDocument(0);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("B"));
		object = test.getDocument(1);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("O"));
		object = test.getDocument(2);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("O"));
		query = "{\r\n" + "	\"blood\": \"A\"\r\n" + "}";
		test.remove(Document.parse(query), true);
		assertTrue(test.count() == 3);
		query = "{\r\n" + "	\"birth.month\": 7\r\n" + "}";
		test.remove(Document.parse(query), true);
		assertTrue(test.count() == 2);
		object = test.getDocument(0);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("B"));
		JsonObject birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("month").getAsDouble() == 6);
		object = test.getDocument(1);
		assertTrue(object.getAsJsonPrimitive("blood").getAsString().equals("O"));
		birth = object.getAsJsonObject("birth");
		assertTrue(birth.getAsJsonPrimitive("month").getAsDouble() == 6);
		db.dropDatabase();
	}
	
	@AfterAll
	public static void clean() {
		DB db1 = new DB("insert");
		db1.dropDatabase();
		DB db2 = new DB("remove");
		db2.dropDatabase();
		DB db3 = new DB("update");
		db3.dropDatabase();
		DB db4 = new DB("drop");
		db4.dropDatabase();
	}
}