package core;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;

public class ReadRangeIterator implements Iterator {
	private HashMap<String, char[]> krazy_keys;
	private String temp_path;
	private File temp_file;
	private KVPair next;
	
	public ReadRangeIterator(HashMap<String, char[]> krazy_keys, String temp_path, File temp_file) {
		this.krazy_keys = krazy_keys;
		this.temp_path = temp_path;
		this.temp_file = temp_file;
		this.next = null;
	}
	
	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public Object next() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
