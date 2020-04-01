package mongo;

import java.io.File;

public class DB {
	public static final String dir = "testfiles/";
	
	private String name;
	private String path;
	private File file;

	/**
	 * Creates a database object with the given name.
	 * The name of the database will be used to locate
	 * where the collections for that database are stored.
	 * For example if my database is called "library",
	 * I would expect all collections for that database to
	 * be in a directory called "library".
	 * 
	 * If the given database does not exist, it should be
	 * created.
	 */
	public DB(String name) {
		this.name = name;
		this.path = dir + name;
		this.file = new File(path);
		if(!this.file.exists()) {
			this.file.mkdir();
		}
	}
	
	public String getName() {
		return name;
	}

	public String getPath() {
		return path;
	}

	public File getFile() {
		return file;
	}

	/**
	 * Retrieves the collection with the given name
	 * from this database. The collection should be in
	 * a single file in the directory for this database.
	 * 
	 * Note that it is not necessary to read any data from
	 * disk at this time. Those methods are in DBCollection.
	 */
	public DBCollection getCollection(String name) {
		return new DBCollection(this, name);
	}
	
	/**
	 * Drops this database and all collections that it contains
	 */
	public void dropDatabase() {
		if(this.file.exists()) {
			String[] delFileNames = this.file.list();
			for (String delFileName : delFileNames) {
				File delFile = new File(this.file.getPath(), delFileName);
				delFile.delete();
			}
			this.file.delete();
		}
	}
	
	
}
