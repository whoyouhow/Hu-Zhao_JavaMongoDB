package mongo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class DBCollection {
	
	public static final String delimiter = "\n";

	public static void print(Object o) {
//		System.out.println(o);
	}
	
	
	private DB database;
	private String name;
	private String path;
	private File file;
	private ArrayList<JsonObject> docPool;

	/**
	 * Constructs a collection for the given database
	 * with the given name. If that collection doesn't exist
	 * it will be created.
	 */
	public DBCollection(DB database, String name) {
		this.database = database;
		this.name = name;
		this.path = this.database.getPath() + "/" + name + ".json";
		this.file = new File(path);
		this.docPool = new ArrayList<JsonObject>();
		if(!this.file.exists()) {
			try {
				this.file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		ArrayList<String> docStrs = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		try {
			FileReader fr = new FileReader(this.path);
			BufferedReader br = new BufferedReader(fr);
//			String line = br.readLine();
			String line;
			while((line = br.readLine()) != null) {
				if(line.trim().length() != 0) {
					sb.append(line);
					
				}
				else {
					docStrs.add(new String(sb));
//					docStrs.add(sb.toString());
					sb.delete(0, sb.length());
				}
			}
			if(sb.length() > 0) {
				docStrs.add(new String(sb));
//				docStrs.add(sb.toString());
			}
				
				br.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		for(String docStr: docStrs) {

			this.docPool.add(Document.parse(docStr));
			
//			System.out.println(docStr);
		}
//		System.out.println(Document.parse(docStr));
		
	}
	
	/**
	 * Returns a cursor for all of the documents in
	 * this collection.
	 */
	public DBCursor find() {
		DBCursor cursor = new DBCursor(this, null, null);
		return cursor;
	}
	
	/**
	 * Finds documents that match the given query parameters.
	 * 
	 * @param query relational select
	 * @return
	 */
	public DBCursor find(JsonObject query) {
		DBCursor cursor = new DBCursor(this, query, null);
		return cursor;
	}
	
	/**
	 * Finds documents that match the given query parameters.
	 * 
	 * @param query relational select
	 * @param projection relational project
	 * @return
	 */
	public DBCursor find(JsonObject query, JsonObject projection) {
		DBCursor cursor = new DBCursor(this, query, projection);
		return cursor;
	}
	
	private void write() {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(this.file, false));
			for(JsonObject doc: this.docPool) {
				writer.write(gson.toJson(doc));
				writer.newLine();
				writer.newLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				print("failed to close!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				e.printStackTrace();
			}
		}
	}

	public void writee() {
//		for(JsonObject doc: docPool) {
//			this.write(doc);
//		}
		StringBuilder content = new StringBuilder();
		FileWriter fw = null;
		try { 
			fw = new FileWriter(this.file);
			int docPoolSize = this.docPool.size();
			for(int i=0; i<docPoolSize; i++) {
				content.append(this.docPool.get(i).toString());
				if(i != docPoolSize-1) {
					content.append(delimiter);
				}
			}
			fw.write(content.toString());
			fw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {

			try {
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
			
	}
	public void writee(JsonObject doc) {
		StringBuilder content = new StringBuilder();
		try {
			FileWriter fw = new FileWriter(this.file);
			content.append(doc.toString());
			content.append(delimiter);
			fw.write(content.toString());
			fw.flush();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Inserts documents into the collection
	 * Must create and set a proper id before insertion
	 * When this method is completed, the documents
	 * should be permanently stored on disk.
	 * @param documents
	 */
	public void insert(JsonObject... documents) {
		for(JsonObject doc : documents) {
			if (!doc.has("_id")) {
				doc.addProperty("_id", UUID.randomUUID().toString());
			}
			this.docPool.add(doc);
			
//			this.writeJsonObject(doc, true);
			
		}
		this.write();
	}
	
	/**
	 * Locates one or more documents and replaces them
	 * with the update document.
	 * @param query relational select for documents to be updated
	 * @param update the document to be used for the update
	 * @param multi true if all matching documents should be updated
	 * 				false if only the first matching document should be updated
	 */
	public void update(JsonObject query, JsonObject update, boolean multi) {
		DBCursor matched = this.find(query);
//		print(matched.getDocPool().toString());
		if (matched.count()==0) {
			return;
		}
		
		ArrayList<String> strings = new ArrayList<String>();
		ArrayList<String> key = new ArrayList<String>();
		ArrayList<String> value = new ArrayList<String>();
		ArrayList<String> newValue = new ArrayList<String>();
		
		JsonObject update_content = update.get("$set").getAsJsonObject();
		print(update_content);
		for(String s: update_content.toString().split("\\,")) {
			s = s.replace(" ", "");
			s = s.replace("{", "");
			s = s.replace("}", "");
//			s = "{" + s + "}";
			strings.add(s);
			
		}
		for (int i =0;i<strings.size();i++) {
			String string = strings.get(i);
			key.add (string.split("\\:")[0]);
			value.add(string.split("\\:")[1]);
			
		}
		
		if(!multi) {
			JsonObject jj = matched.getDocPool().get(0);
			String newstring = jj.toString();
			for (int j = 0; j < key.size();j++) {
				
				String kk = key.get(j);
				kk = kk.replace("\"", "");
				String[] tw = kk.split("\\.");
//				print(key.get(j));
				JsonObject deepestDoc = matched.getDeepestLayer(jj, tw);
				JsonElement v = deepestDoc.get(tw[tw.length-1]);
//				print(v);
				newValue.add(v.toString());
			}
			for(int j = 0; j<key.size();j++) {
				newstring = newstring.replace(newValue.get(j), value.get(j));
			}		
			
			JsonObject modified = Document.parse(newstring);
			docPool.set(docPool.indexOf(jj), modified);
		}
		else {
			ArrayList<JsonObject> jj = new ArrayList<JsonObject>();
			ArrayList<String> newStrings = new ArrayList<String>();
			for(int i = 0; i< matched.getDocPool().size();i++) {
				
				jj.add( matched.getDocPool().get(i));
				newStrings.add( jj.get(i).toString());
				
			}
			
			for (int i =0; i< jj.size();i++) {
				for (int j = 0; j < key.size();j++) {
				
					String kk = key.get(j);
					kk = kk.replace("\"", "");
					String[] tw = kk.split("\\.");
//					print(key.get(j));
					JsonObject deepestDoc = matched.getDeepestLayer(jj.get(i), tw);
					JsonElement v = deepestDoc.get(tw[tw.length-1]);
//					print(v);
					newValue.add(v.toString());
				}
			}
			
			for(int j = 0; j<key.size();j++) {
				for (int i = 0; i<value.size();i++) {
					newStrings.set(j, newStrings.get(j).replace(newValue.get(i), value.get(i)));
				}
				newValue.remove(0);
				newValue.remove(0);
			}
			
			for(int j = 0; j<jj.size();j++) {
				JsonObject modified = Document.parse(newStrings.get(j));
				docPool.set(docPool.indexOf(jj.get(j)), modified);
			}
		}
		print(docPool.toString());
		this.write();
		
		
		
		
		
		
		
	}
	
	/**
	 * Removes one or more documents that match the given
	 * query parameters
	 * @param query relational select for documents to be removed
	 * @param multi true if all matching documents should be updated
	 * 				false if only the first matching document should be updated
	 */
	public void remove(JsonObject query, boolean multi) {
		DBCursor matched = this.find(query);
		while(matched.hasNext()) {
			JsonObject docToDel = matched.next();
			this.docPool.remove(docToDel);
			if(!multi) {
				break;
			}
		}
		this.write();
//		for(JsonObject doc:this.docPool) {
//			this.writeJsonObject(doc, true);
//		}
	}
	
	/**
	 * Returns the number of documents in this collection
	 */
	public long count() {
		return this.docPool.size();
	}
	
	
	
	
	

	
	public String getPath(){
		return this.path;
	}
	
	public ArrayList<JsonObject> getDocPool() {
		ArrayList<JsonObject> docPoolCopy = new ArrayList<JsonObject>();
		for(JsonObject doc: this.docPool) {
			docPoolCopy.add(doc);
		}
		return docPoolCopy;
	}
	
	public Iterator<JsonObject> getDocPoolIter() {
		return this.docPool.iterator();
	}
	
	public String getName() {
		return this.name;
	}
	
	/**
	 * Returns the ith document in the collection.
	 * Documents are separated by a line that contains only a single tab (\t)
	 * Use the parse function from the document class to create the document object
	 */
	public JsonObject getDocument(int i) {
		return i < this.docPool.size()? this.docPool.get(i) : null;
	}
	
	/**
	 * Drops this collection, removing all of the documents it contains from the DB
	 */
	public void drop() {
		print("!!!drop");
		if(this.file.exists()) {
			print("exists:"+this.file.getPath());
			print(this.file.getAbsolutePath());

			InputStream inputStream;
			try {
				inputStream = new FileInputStream(this.file);
				
				if (inputStream != null) {
		            inputStream.close();
		        }
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			boolean success = this.file.delete();
			print(success);
			print(this.file.exists());
			if(!success) {
				print("not suceess");
				File delFile = new File(this.getPath());
				boolean success2 = delFile.delete();
				print(success2);
			}
		}
	}
	
}
