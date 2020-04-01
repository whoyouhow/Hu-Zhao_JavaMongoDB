package mongo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonArray;

public class DBCursor implements Iterator<JsonObject>{
	
	public static void print(Object o) {
//		System.out.println(o);
	}
	
	private DBCollection collection;
	private JsonObject query;
	private JsonObject fields;
	private Iterator<JsonObject> iterCollection;
	private Iterator<JsonObject> iterCursor;
//	private ArrayList<JsonObject> docPoolCopy;
	private ArrayList<JsonObject> docPoolCursor;
	
	public DBCursor(DBCollection collection, JsonObject query, JsonObject fields) {
		this.collection = collection;
		this.query = query;
		this.fields = fields;
//		this.docPoolCopy = collection.getDocPool();
		this.docPoolCursor = new ArrayList<JsonObject>();
		this.iterCollection = collection.getDocPoolIter();
		
		if(query == null && fields == null) {
			this.docPoolCursor = collection.getDocPool();
		}
		
		if(query != null) {
			this.match(query);
			if(fields != null) {
				this.project(fields);
			}
		}
		
		this.iterCursor = docPoolCursor.iterator();
	}
	
	/**
	 * Returns true if there are more documents to be seen
	 */
	public boolean hasNext() {
		return this.iterCursor.hasNext()? true : false;
	}

	/**
	 * 
	 * Returns the next document
	 */
	public JsonObject next() {
		return this.iterCursor.next();
	}
	
	/**
	 * Returns the total number of documents
	 */
	public long count() {
		return this.docPoolCursor.size();
	}
	
	public void project(JsonObject projection) {
		print("!!!!project");
		print(projection.toString());
		Iterator<String> fieldIter = projection.keySet().iterator();
		ArrayList<String> projectedFields = new ArrayList<String>();
//		ArrayList<JsonObject> newDocPool = new ArrayList<JsonObject>();
		projectedFields.add("_id");
		while(fieldIter.hasNext()) {
			String field = fieldIter.next();
			if(projection.get(field).getAsString().equals("0") || projection.get(field).getAsString().equals("false")) {
			//excluding multiple fields is illegal	
				print("!!!!project0");
				for(JsonObject doc: this.docPoolCursor) {
					doc.remove(field);
				}
			}
			else {//1 or true to include field
				print("!!!!project1");
				projectedFields.add(field);
			}
		}
		for(JsonObject doc: this.docPoolCursor) {//for each doc
			Iterator<String> docFieldIter = doc.keySet().iterator();
			ArrayList<String> delFields = new ArrayList<String>();
			while(docFieldIter.hasNext()) {//search through the fields of the doc
				//find out the fields in doc that are not in projection
				String docField = docFieldIter.next();
				if(!projectedFields.contains(docField)) {
					delFields.add(docField);
				}
			}
			for(String delField: delFields) {
				doc.remove(delField);
			}
		}
		
	}
	
	public void matchh(JsonObject query) {
		boolean isToRemove = false;
		JsonObject matchedDoc = null;
//		for(JsonObject doc: this.docPoolCursor) {
		while(this.iterCollection.hasNext()) {
			JsonObject doc = this.iterCollection.next();
			Iterator<String> keyIter = query.keySet().iterator();
			int i = 0;
			while(keyIter.hasNext()) {
				i++;
				System.out.println("i="+i);
				String key = keyIter.next();

				print(this.docPoolCursor.size());
				if(doc.has(key)) {
					if(query.get(key).isJsonPrimitive()) {
						if(this.matchComparison(doc.get(key).getAsJsonPrimitive(), query.get(key).getAsJsonPrimitive())) {
							this.docPoolCursor.add(doc);
						}
					}
					else if(query.get(key).isJsonObject()) {
						if(this.matchComparison(doc.get(key).getAsJsonPrimitive(), query.get(key).getAsJsonObject())) {
							this.docPoolCursor.add(doc);
						}
					}
					
				}
				print(this.docPoolCursor.size());
			}
		}
	}
	

	public void match(JsonObject query) {
//		for(JsonObject doc: this.docPoolCursor) {
		while(this.iterCollection.hasNext()) {
			JsonObject doc = this.iterCollection.next();
			
			Iterator<String> keyIter = query.keySet().iterator();
			int i = 0;
			while(keyIter.hasNext()) {
				i++;
//				System.out.println("i="+i);
				String key = keyIter.next();
				String[] keyLayers = key.split("\\.");
				JsonObject deepestDoc = this.getDeepestLayer(doc, keyLayers);
				print(this.docPoolCursor.size());
				if(deepestDoc == null) {
					print("!!null");
					continue;
				}
				print(doc.toString());
				print(deepestDoc.toString());
				print(keyLayers[keyLayers.length - 1]);
				print(query.get(key));
				String deepestKey = keyLayers[keyLayers.length - 1];
				if(deepestDoc.has(deepestKey)) {
					if(query.get(key).isJsonPrimitive()) {
						if(this.matchComparison(deepestDoc.get(deepestKey).getAsJsonPrimitive(), query.get(key).getAsJsonPrimitive())) {
							this.docPoolCursor.add(doc);
						}
					}
					else if(query.get(key).isJsonObject()) {
						print("isdoc");
						print(deepestDoc.get(deepestKey));
						print(query.get(key).getAsJsonObject());
						if(this.matchComparison(deepestDoc.get(deepestKey).getAsJsonPrimitive(), query.get(key).getAsJsonObject())) {
							this.docPoolCursor.add(doc);
							print("true");
						}
					}
					
				}
				print("finish");
//				print(this.docPoolCursor.size());
			}
			
		}
	}
	
	public JsonObject getDeepestLayer(JsonObject doc, String[] keyLayers) {
		print("deep");
		JsonObject deepestLayer = doc;
		int i = 0;
		while(i < keyLayers.length - 1) {
			print("i="+i+" keyLayers[i]"+ keyLayers[i]);
			
			if(deepestLayer.has(keyLayers[i])) {
				if((deepestLayer.get(keyLayers[i])).isJsonObject()){//next layer is a doc
					print("good");
					deepestLayer = deepestLayer.get(keyLayers[i]).getAsJsonObject();
					print(deepestLayer.toString());
				}
				else {
					return null;
				}
			}
			else {
				return null;
			}
			i++;

		}
		return deepestLayer;
	}

	public boolean matchComparison(JsonPrimitive docV, JsonPrimitive queryV) {
		print("!!!!!primitives");
		print(docV.toString());
		print(queryV.toString());
		print(docV.equals(queryV));
		return docV.equals(queryV);
	}
	public boolean matchComparison(JsonPrimitive docV, JsonObject queryV) {
		print("matchcomp");
		Iterator<String> compsIter = queryV.keySet().iterator();
		while(compsIter.hasNext()) {
			//syntax:{comp:compV} 
			//example:{$eq:"good"}
			//example:{$gte:5}
			String comp = compsIter.next();
			if(queryV.get(comp).isJsonPrimitive()) {
				print("primitive");
				print(comp);
				JsonPrimitive compV = queryV.get(comp).getAsJsonPrimitive();
				if(comp.equals("$eq")) {
					return docV.equals(compV);
				}
				else if(comp.equals("$ne")) {
					return !docV.equals(compV);
				}
				else if(comp.equals("$gt")) {
					if(docV.isNumber() && compV.isNumber()) {
						return compV.getAsNumber().doubleValue() > docV.getAsNumber().doubleValue();
					}
					else if(docV.isString() && compV.isString()) {
						return compV.getAsString().compareTo(docV.getAsString()) > 0;
					}
					else {
						return false;
					}
				}
				else if(comp.equals("$gte")) {
					if(docV.isNumber() && compV.isNumber()) {
						return compV.getAsNumber().doubleValue() >= docV.getAsNumber().doubleValue();
					}
					else if(docV.isString() && compV.isString()) {
						return compV.getAsString().compareTo(docV.getAsString()) >= 0;
					}
					else {
						return false;
					}
				}
				else if(comp.equals("$lt")) {
						print("lt num");
					if(docV.isNumber() && compV.isNumber()) {
						return docV.getAsNumber().doubleValue() < compV.getAsNumber().doubleValue();
					}
					else if(docV.isString() && compV.isString()) {
						return docV.getAsString().compareTo(compV.getAsString()) < 0;
					}
					else {
						return false;
					}
				}
				else if(comp.equals("$lte")) {
					if(docV.isNumber() && compV.isNumber()) {
						return compV.getAsNumber().doubleValue() <= docV.getAsNumber().doubleValue();
					}
					else if(docV.isString() && compV.isString()) {
						return compV.getAsString().compareTo(docV.getAsString()) <= 0;
					}
					else {
						return false;
					}
				}
				
			}//end of compV is primitive
			else if(queryV.get(comp).isJsonArray()) {
				JsonArray compV = queryV.get(comp).getAsJsonArray();
				if(comp.equals("$in")) {
					for(JsonElement arrV: compV) {
						if(docV.equals(arrV)) {
							return true;
						}
					}
					return false;
				}
				if(comp.equals("$nin")) {
					for(JsonElement arrV: compV) {
						if(docV.equals(arrV)) {
							return false;
						}
					}
					return true;
				}
			}
		}
		return false;
	}
	
	public ArrayList<JsonObject> getDocPool() {
		ArrayList<JsonObject> docPoolCopy = new ArrayList<JsonObject>();
		for(JsonObject doc: this.docPoolCursor) {
			docPoolCopy.add(doc);
		}
		return docPoolCopy;
	}
	

}
