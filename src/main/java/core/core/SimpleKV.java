package core;
import java.io.*;
import java.util.*;

public class SimpleKV implements KeyValue {
	private HashMap<String, char[]> krazy_keys;
	private String pathfile;
	private int tid;
	private String temp_path;
	private boolean temp_populated;
	
	/*
	 * Trade off of having it all fit in memory vs efficient for everything
	 * if could have infinite main memory, then i want a hashmap for inserts/lookups
	 * and then a treemap for the range queries
	 * and the main storage as an array list of kv pairs
	 * */

    public SimpleKV() {
    	this.krazy_keys = new HashMap<String, char[]>();
    	this.tid = 0;
    	this.temp_populated = false;
    }
    
    public String get_actual_path() {
    	return this.pathfile;
    }
    public String get_path() {
    	return this.temp_path;
    }
    
    public double get_memory() {
    	return this.get_size()*0.00042;
    }
    
    public int get_size() {
    	return this.krazy_keys.size();
    	
    }

    @Override
    public SimpleKV initAndMakeStore(String path) {
    	SimpleKV kv = new SimpleKV();
    	if (path !=  null) {
    		//Initializing actual_file at given path
    		String dir = System.getProperty("user.dir");
    		kv.pathfile = dir+path+".txt";
    		File files = new File(kv.pathfile);
    		try {
				files.createNewFile();
			} catch (IOException e1) {
				System.out.println("file failed");
			}
    		//Initializing temp_file at txn 0
    		kv.temp_path = dir+"/transaction"+kv.tid+".txt";
    		File clearout = new File(kv.temp_path);
    		clearout.delete();
    		File tfile = new File(kv.temp_path);
    		try {
				tfile.createNewFile();
			} catch (IOException e1) {
				System.out.println("temp fails");
			}
    		try {
				BufferedReader br = new BufferedReader( new FileReader(new File(kv.pathfile)));
				BufferedWriter bw = new BufferedWriter( new FileWriter(tfile));
				String s; 
				while ((s = br.readLine()) != null) {
					if (kv.get_memory() > 25) {
						//write to temp file
						bw.write(s);
						bw.newLine();
					}else {
						//add it to the kv krazy_keys store
						String[] arrofpair = s.split(" , ");
						kv.krazy_keys.put(arrofpair[0], arrofpair[1].toCharArray());
					}
				}
				br.close();
				bw.close();
				kv.temp_populated = true;
				
			} catch (FileNotFoundException e) {
				//throwing the error for buffered reader of actual file
				System.out.println("No file found!");
			} catch (IOException e) {
				System.out.println("Readline of buffered reader failed");
			}
    	}
    	return kv;
    }

    @Override
    public void write(char[] key, char[] value) {
    		String k_string = new String(key);
    		// Check if key in hashMap or if hashMap has space
    		if (this.krazy_keys.containsKey(k_string) || this.get_memory() < 25) {
    			this.krazy_keys.put(k_string, value);
    		} // Flush half of hashMap to temp and insert new KV pair into hashMap
    		else {
    			int count = 0;
    			int goal = (int) (this.krazy_keys.size()/2.0);
    			Object[] keyarr = this.krazy_keys.keySet().toArray();	
    			try {
    				BufferedWriter temp_br = new BufferedWriter(new FileWriter(this.temp_path, true));
	    			for (Object oevict : keyarr) {
	    				String keyToEvict = (String) oevict;
	    				//Write evicted pair to end of temp file
	    				temp_br.write(keyToEvict + " , " + new String(this.krazy_keys.get(keyToEvict)));
	    				temp_br.newLine();
	    				
	    				this.krazy_keys.remove(keyToEvict);
	    				count++;
	    				if (count > goal)
	    					break;
	    			}
	    			temp_br.close();
    			} catch (IOException e1) {
					System.out.println("Unable to open/use temp file");
				}
    			this.krazy_keys.put(k_string, value);

    		}
    }

    @Override
    public char[] read(char[] key) {
    	String k_string = new String(key);
    	if (this.krazy_keys.containsKey(k_string)) {
    		//Case: in hashmap
    		return this.krazy_keys.get(k_string);
    	}
    	else {
    		//Case:not in hashmap, check in temp file
    		try {
    			File tf = new File(this.temp_path);
        		BufferedReader br = new BufferedReader(new FileReader(tf));
        		String s;
        		String best = new String();
				while ((s=br.readLine())!=null) {
					if (s.startsWith(k_string+" , ")) {
						//Gets most up to date value from temp file
						String[] arrofpair = s.split(" , ");
						best =  arrofpair[1];
					}
				}
				br.close();
				if (best.length() != 0) {
					//return best value of temp file
					return best.toCharArray();
				}
				else {
					System.out.println("Reached end of temp and hashmap, but no value:(");
				}
			} catch (IOException e) {
				System.out.println("Buffered reader in Read operationfailed");
			}
    		return null; //ideally dont reach here
    	}

    }
    
    public void flush() {
    	Object[] keyarr = this.krazy_keys.keySet().toArray();
    	//evict entire hashmap to disk
    	try {
			BufferedWriter temp_br = new BufferedWriter(new FileWriter(this.temp_path, true));
			for (Object oevict : keyarr) {
				String keyToEvict = (String) oevict;
				temp_br.write(keyToEvict + " , " + new String(this.krazy_keys.get(keyToEvict)));
				temp_br.newLine();				
				this.krazy_keys.remove(keyToEvict);
			}
			temp_br.close();
		} catch (IOException e1) {
			System.out.println("Unable to open/use temp file");
		}
    	keyarr = null; //"delete" the keys array
    }

    @Override
    public Iterator<KVPair> readRange(char[] startKey, char[] endKey) {
    	flush();
    	return new KVReadRangeIterator(this.temp_path, new String(startKey), new String(endKey));
    }

    @Override
    public void beginTx() {
    	if (!temp_populated) {
    		//must populated ourselves
    		File temp_file = new File(this.temp_path);
    		this.krazy_keys = new HashMap<String, char[]>();
    		try {
				BufferedReader br = new BufferedReader(new FileReader(new File(this.pathfile)));
				BufferedWriter bw = new BufferedWriter(new FileWriter(temp_file));
				
				String s; 
				try {
					while ((s = br.readLine()) != null) {
						if (this.get_memory() > 25) {
							//write to temp file
							bw.write(s);
							bw.newLine();
						}else {
							//add it to the kv krazy_keys store
							String[] arrofpair = s.split(" , ");
							this.krazy_keys.put(arrofpair[0], arrofpair[1].toCharArray());
						}
					}
					br.close();
					bw.close();
					
				}  catch (IOException e1) {
					// TODO Auto-generated catch block
					System.out.println("Buffers failing in begin txn");
				}
				
			} catch (FileNotFoundException e) {
				System.out.println("Failed to make buffered reader for actual in begin TXN");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("buffered read of temp fialing in begin txn");
			}
    		
    	}
    	
    }
    @Override
    public void commit() {
    	//Iterate through hashmap and append all new values to temp_file
    	try {
			BufferedWriter temp_bw = new BufferedWriter(new FileWriter(this.temp_path));
			for (String k : this.krazy_keys.keySet()) {
				String line = k + " , "+new String(this.krazy_keys.get(k));
				temp_bw.write(line);
				temp_bw.newLine();
			}
			temp_bw.close();
		} catch (IOException e1) {
			System.out.println("Failed to make buffered writer for temp in commit");
		}
		
		//Delete original contents of actual_file
		File af = new File(this.pathfile);
		af.delete();
		
		//create empty file at actual_file_path
		File afile = new File(this.pathfile);
		
		try {
			afile.createNewFile();
		} catch (IOException e1) {
			System.out.println("Creating new actual file");
		}
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(afile));
			BufferedReader temp_br = new BufferedReader(new FileReader(new File(this.temp_path)));
			String a;
			while ((a=temp_br.readLine())!= null) {
				bw.write(a);
				bw.newLine();	
			}
			bw.close();
			temp_br.close();
		} catch (IOException e) {
			System.out.println("Failing to read line from temp file in commit");
		}
		
		this.tid++;
		
		//clear out temp files because now on disk and dont want silly errors
		File to_remove = new File(this.temp_path);
		to_remove.delete();
		
		//Set new temp path with incremented txn id
    	String still_dir = System.getProperty("user.dir");
    	this.temp_path = still_dir+"/transaction"+this.tid+".txt";
		File tfile = new File(this.temp_path);
		try {
			tfile.createNewFile();
		} catch (IOException e) {
			System.out.println("commit: new txn temp_file failed to create");
		}
		this.temp_populated=false;
    	
    }
    
    public class KVReadRangeIterator implements Iterator<KVPair> {
    	private final String temp_path;
    	private final HashSet<String> keys_from_temp;
    	private int temp_linecount;
    	private KVPair next;
    	private String start;
    	private String end;

    	
    	public KVReadRangeIterator(String temp_path, String start, String end) {
    		this.temp_path = temp_path;
    		this.keys_from_temp = new HashSet<>();
    		this.temp_linecount = 0; 
    		this.next = null;
    		this.start = start;
    		this.end = end;
    	}
    	
    	@Override
    	public boolean hasNext() {
    		String key = "";
    		// First iterate through Hashmap
    		
//    		while (this.index_in_map < this.keys_from_map.length) {
//    			key = (String) this.keys_from_map[index_in_map];
//    			boolean in_range = key.compareTo(start) >=0 && key.compareTo(end)<=0;
//    			this.index_in_map++;
//    			if (in_range) {
//    				this.next = new KVPair(key.toCharArray(), krazy_keys.get(key));
//    				return true;
//    			}
//    			
//    		}
    			
    		 // Then iterate through temp, ignoring keys seen in HashMap  
    	
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
    						if (!(krazy_keys.containsKey(arrofpair[0]) || this.keys_from_temp.contains(arrofpair[0]))) {
    							if(arrofpair[0].compareTo(start) >=0 && arrofpair[0].compareTo(end)<=0) {
    								key = arrofpair[0];
    								value = arrofpair[1];
    								found_new_key = true;
    								this.keys_from_temp.add(key);
    								this.temp_linecount = i; 
    							}
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
    			System.out.println("Couldn't read temp file line in readRange");
    		}
    		// no more values 
    		return false;
    	}

    	@Override
    	public KVPair next() {
    		return this.next;
    	}
    	
    }
    
}
