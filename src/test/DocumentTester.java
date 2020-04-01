package test;

import static org.junit.Assert.*;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import mongo.Document;

public class DocumentTester {
	
	@Test
	public void testParse() {
		String json = "{ \"key\": \"value\" }";
		JsonObject results = Document.parse(json);
		assertTrue(results.getAsJsonPrimitive("key").getAsString().equals("value"));
	}
	
	@Test
	public void testEmbedded() {
		String json =  "{ \"key\": \"value\", \"embedded\":{\"key2\":\"value2\"}}";
		JsonObject results = Document.parse(json);
		JsonObject oj = new JsonObject();
		oj.addProperty("key2", "value2");
		assertTrue(results.getAsJsonObject("embedded").equals(oj));
	}
	
	@Test
	public void testArray() {
		String json =  "{ \"key\": \"value\", \"Array\":[\"one\",\"two\",\"three\"]}";
		JsonObject results = Document.parse(json);
		JsonArray a = new JsonArray();
		a.add("one");
		a.add("two");
		a.add("three");
		assertTrue(results.getAsJsonArray("Array").equals(a));
		
	}

}