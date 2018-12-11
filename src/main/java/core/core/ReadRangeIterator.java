package core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class ReadRangeIterator implements Iterator<KVPair> {
	private final HashMap<String, char[]> krazy_keys;
	private final Object[] keys_from_map;
	private int index_in_map;
	private final String temp_path;
	private final HashSet<String> keys_from_temp;
	private int temp_linecount;
	private KVPair next;
	
	public ReadRangeIterator(HashMap<String, char[]> krazy_keys, String temp_path) {
		this.krazy_keys = krazy_keys;
		this.keys_from_map = this.krazy_keys.keySet().toArray();
		this.index_in_map = 0; 
		this.temp_path = temp_path;
		this.keys_from_temp = new HashSet<>();
		this.temp_linecount = 0; 
		this.next = null;
	}
	
	@Override
	public boolean hasNext() {
		String key = "";
		// First iterate through Hashmap
		if (this.index_in_map < this.keys_from_map.length) {
			key = (String) this.keys_from_map[index_in_map];
			this.next = new KVPair(key.toCharArray(), this.krazy_keys.get(key));
			return true;
		} // Then iterate through temp, ignoring keys seen in HashMap  
		else {
			File temp_file = new File(this.temp_path);
			try {
				BufferedReader br = new BufferedReader(new FileReader(temp_file));
				String s;
				int i = 0; 
				boolean found_new_key = false;
				String value = "";
				// read through entire temp file
				while ((s = br.readLine()) != null) { 
					// ignore lines that are before the last new key 
					if (i > this.temp_linecount) {
						String[] arrofpair = s.split(" , ");
						// if new key hasn't been found, then keep searching for one
						if (!found_new_key) {
							// if key not in HashMap or in keys seen from temp, then store this new key
							if (!(this.krazy_keys.containsKey(arrofpair[0]) || this.keys_from_temp.contains(arrofpair[0]))) {
								key = arrofpair[0];
								value = arrofpair[1];
								found_new_key = true;
								this.keys_from_temp.add(key);
								this.temp_linecount = i; 
							}
						} // if new key found, then check if current line contains updated value 
						else {
							if (arrofpair[0].equals(key)) {
								value = arrofpair[1];
							}
						}
					} 
					i++; 
				}
				br.close();
				if (found_new_key) {
					this.next = new KVPair(key.toCharArray(), value.toCharArray());
					return true;
				}
				return false;
			} catch (FileNotFoundException e) {
				System.out.println("Couldn't open temp file in readRange");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Couldn't read temp file line in readRange");
			}
			
		}
		// no more values 
		return false;
	}

	@Override
	public KVPair next() {
		return this.next();
	}
	
}
