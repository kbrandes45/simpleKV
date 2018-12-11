package core;
import java.io.*;
import java.util.*;
import java.util.Iterator;

public class SimpleKV implements KeyValue {
	private HashMap<String, char[]> krazy_keys;
	private String pathfile;
	//private File actual_file;
	private int tid;
	private String temp_path;
	private boolean temp_populated;
	//private File temp_file;
	
	/*
	 * Trade off of having it all fit in memory vs efficient for everything
	 * if could have infinite main memory, then i want a hashmap for inserts/lookups
	 * and then a treemap for the range queries
	 * and the main storage as an array list of kv pairs
	 * */

    public SimpleKV() {
    	//this.simple_kv = new ArrayList<KVPair>();
    	//this.key_map = new TreeMap<String, Integer>();
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
    
    public void help_overwrite(String path) {
    	this.temp_path = path;
    	//this.temp_file = new File(path);
    }
    
    public double get_memory() {
    	return this.get_size()*0.000032;
    }
    
    public int get_size() {
    	return this.krazy_keys.size();
    	
    }

    @Override
    public SimpleKV initAndMakeStore(String path) {
    	SimpleKV kv = new SimpleKV();
    	if (path !=  null) {
    		String dir = System.getProperty("user.dir");
    		kv.pathfile = dir+path+".txt";
    		File files = new File(kv.pathfile);
    		try {
				files.createNewFile();
			} catch (IOException e1) {
				System.out.println("file failed");
			}
    		
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
					if (kv.get_memory() > 50) {
						//write to temp file
						System.out.println("Write to temp!");
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
				//File a = new File (kv.pathfile);
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
    		// >>>>>> CHANGE TO < 500 MB WHICH IS 524288000 bytes <<<<<
    		if (this.krazy_keys.containsKey(k_string) || this.get_memory() < 50) {
    			this.krazy_keys.put(k_string, value);
    		} // Evict Pair from hashMap to temp and insert new KV pair into hashMap
    		else {
    			//System.out.println("key: "+k_string);
    			//System.out.println("Starting eviction case"+this.krazy_keys.size()+" mem "+Runtime.getRuntime().freeMemory());
    			int count = 0;
    			int goal = (int) (this.krazy_keys.size()/2.0);
    			Object[] keyarr = this.krazy_keys.keySet().toArray();	
    			try {
    				BufferedWriter temp_br = new BufferedWriter(new FileWriter(this.temp_path, true));
	    			for (Object oevict : keyarr) {
	    				String keyToEvict = (String) oevict;
	    				//System.out.println("Write to temp: "+ keyToEvict);
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
    			//System.out.println("Eviction complete+ "+this.krazy_keys.size()+ " mem "+Runtime.getRuntime().freeMemory());
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

    @Override
    public Iterator<KVPair> readRange(char[] startKey, char[] endKey) {
    	return new ReadRangeIterator(this.krazy_keys, this.temp_path, new String(startKey), new String(endKey));
//    	ArrayList<KVPair> temp_pairs = new ArrayList<KVPair>(); // most up to date KV pairs
//    	//Map = key string, index at which respective kv pair is in temp_pair
//    	HashMap<String, Integer> indexes = new HashMap<String, Integer>(); 
//    	String started = new String(startKey);
//    	String ending = new String(endKey);
//    	//If the start key is greater than the endkey
//    	if (started.compareTo(ending) > 0)
//    		return null;
//    	int count = 0;
//    	//Iterating over hashmap
//    	for (String k : this.krazy_keys.keySet()) {
//    		char[] next_kvp_e2 = this.krazy_keys.get(k);
//    		boolean in_range = k.compareTo(started) >=0 && k.compareTo(ending)<=0;
//    		if (in_range) {
//    			//Add value to temp_pairs (and its index) 
//    			KVPair next_kvp = new KVPair(k.toCharArray(), next_kvp_e2);
//    			temp_pairs.add(next_kvp);
//    			count++;
//    		}  		
//    	}
//    	//Iterate over temp file
//		try {
//			File tf = new File (this.temp_path);
//			BufferedReader br = new BufferedReader(new FileReader(tf));
//	    	String s;
//	    	while ((s = br.readLine())!= null) {
//	    		String[] arrofpair = s.split(" , ");
//	    		boolean in_range = arrofpair[0].compareTo(started) >=0 && arrofpair[0].compareTo(ending)<=0;
//	    		if (in_range) { //Check if key in range
//		    		if (!this.krazy_keys.containsKey(arrofpair[0])) { //Check that key not in hashmap
//		    			//Add new key,value pair from temp_file to temp_pairs (and index)
//		    			KVPair next_kvp = new KVPair(arrofpair[0].toCharArray(), arrofpair[1].toCharArray());
//		    			if (indexes.containsKey(arrofpair[0])) {
//		    				//replace value at this index
//		    				int i = indexes.get(arrofpair[0]);
//		    				temp_pairs.set(i, next_kvp);
//		    			}
//		    			else {
//		    				temp_pairs.add(next_kvp);
//		    				count++;
//		    				indexes.put(arrofpair[0], count);
//		    			}
//		    			
//					}
//	    		}
//	    	}
//		} catch (FileNotFoundException e) {
//			System.out.println("Read range file not found");
//		} catch (IOException e) {
//			System.out.println("Read range readline failed");
//		}
//    	return temp_pairs.iterator();
    }

    @Override
    public void beginTx() {
    	if (!temp_populated) {
    		System.out.println("Repopulate temp bc no crash");
    		//must populated ourselves
    		File temp_file = new File(this.temp_path);
    		this.krazy_keys = new HashMap<String, char[]>();
    		try {
				BufferedReader br = new BufferedReader(new FileReader(new File(this.pathfile)));
				BufferedWriter bw = new BufferedWriter(new FileWriter(temp_file));
				
				String s; 
				try {
					while ((s = br.readLine()) != null) {
						if (this.get_memory() > 50) {
							//write to temp file
							System.out.println("Write to temp!");
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
    	//iterate through all of hashmap and add to end of temp
    	//iterate through all of temp, and write as actual
    	//append values to the end of actual
    		
    	//Iterate through hashmap and append all new values to temp_file
    	try {
			BufferedWriter temp_bw = new BufferedWriter(new FileWriter(this.temp_path));
			for (String k : this.krazy_keys.keySet()) {
				String line = k + " , "+new String(this.krazy_keys.get(k));
				temp_bw.write(line);
				temp_bw.newLine();
				//this.krazy_keys.remove(k);
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
			boolean newa = afile.createNewFile();
			if (!newa) {
				System.out.println("New actual file wasnt created... so not deleted");
			}
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
    
}
