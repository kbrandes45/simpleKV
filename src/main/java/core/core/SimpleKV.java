package core;
import java.io.*;
import java.util.*;
import java.util.Iterator;

public class SimpleKV implements KeyValue {
	//main storage for kv is the array list with KVPairs
	//private ArrayList<KVPair> simple_kv;
	//treemap to store arraylist index and key - keeps order on string keys, so range is good
	//private TreeMap<String, Integer> key_map;
	
	private HashMap<String, char[]> krazy_keys;
	private String pathfile;
	private File actual_file;
	private int tid;
	private String temp_path;
	private File temp_file;
	
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
    }
    public String get_path() {
    	return this.temp_path;
    }
    
    public void help_overwrite(String path) {
    	this.temp_path = path;
    	this.temp_file = new File(path);
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
    		File file = new File(path);
    		kv.pathfile = path;
    		kv.actual_file = file;
    		
    		kv.temp_path = "/home/kbrandes/simpleKV/src/main/java/core/core/transaction"+kv.tid+".txt";
    		File tfile = new File(kv.temp_path);
    		kv.temp_file = tfile;
    		try {
				BufferedReader br = new BufferedReader( new FileReader(kv.actual_file));
				BufferedWriter bw = new BufferedWriter( new FileWriter(tfile));
				String s; 
				while ((s = br.readLine()) != null) {
					if (kv.get_memory() > 0.000032) {
						//write to temp file
						System.out.println("Write to temp");
						bw.write(s);					
					}else {
						//add it to the kv krazy_keys store
						String[] arrofpair = s.split(" , ");
						kv.krazy_keys.put(arrofpair[0], arrofpair[1].toCharArray());
					}
				}
				br.close();
				bw.close();
				//kv.temp_file = tfile;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				System.out.println("No file found!");
			} catch (IOException e) {
				// TODO Auto-generated catch block
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
    		if (this.krazy_keys.containsKey(k_string) || this.krazy_keys.size()*32.0 < 5*0.000032) {
    			this.krazy_keys.put(k_string, value);
    		} // Evict Pair from hashMap to temp and insert new KV pair into hashMap
    		else {
    			String keyToEvict = (String) this.krazy_keys.keySet().toArray()[0];
    			String valueToEvict = new String(this.krazy_keys.get(keyToEvict));
    			BufferedWriter temp_br;
			try {
				temp_br = new BufferedWriter(new FileWriter(this.temp_file));
				temp_br.write(keyToEvict + " , " + valueToEvict);
				temp_br.close();
			} catch (IOException e1) {
				System.out.println("Unable to open/use temp file");
			}
			this.krazy_keys.remove(keyToEvict); // evict key
			this.krazy_keys.put(k_string, value); // write in new pair 
    		}
	    	
	    	/*
	    	Integer nxt_ind = this.key_map.get(k_string);
	    	//System.out.print(nxt_ind);
	    	if (nxt_ind != null) { //not null, so in the tree
	    		int index = (int) nxt_ind;
	    		this.simple_kv.set(index, kvp);    		
	    	}
	    	else {
	    		//not there so new index
	    		nxt_ind = this.simple_kv.size();
	    		this.key_map.put(k_string, nxt_ind);
	    		this.simple_kv.add(kvp);
	    	}
	    	*/
    }

    @Override
    public char[] read(char[] key) {
    	String k_string = new String(key);
    	if (this.krazy_keys.containsKey(k_string)) {
    		//Case: in hashmap
    		return this.krazy_keys.get(k_string);
    	}
    	else {
    		//Case: in temp file
    		try {
    			System.out.println("Checking temp files"+this.temp_path);
    			//File tempfile = new File(this.temp_path);
        		BufferedReader br = new BufferedReader(new FileReader(this.temp_file));
        		String s;
        		System.out.println("reader made");
        		String best = new String();
				while ((s=br.readLine())!=null) {
					if (s.startsWith(k_string+" , ")) {
						String[] arrofpair = s.split(" , ");
						best =  arrofpair[1];
					}
				}
				br.close();
				if (best.length() != 0) {
					return best.toCharArray();
				}
				else {
					System.out.println("Reached end of temp and hashmap, but no value:(");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Buffered reader in read failed");
			}
    		return null; //ideally dont reach here
    	}

    	/*
    	if (next_kvp != null) {
    		return next_kvp;//.element2;
    	}
    	else {
    		//System.out.println("null val");
    		return null;
    	}
    
    	Integer nxt_ind = this.key_map.get(k_string);
    	if (nxt_ind != null) { //not null, so in the tree
    		//System.out.print(this.simple_kv.size());
    		//System.out.println("Read!"+ new String(this.simple_kv.get(nxt_ind).element2));
    		return this.simple_kv.get(nxt_ind).element2;		
    	}
    	else {
    		//not there so new index
    		//System.out.println("Read!");
    		return null;
    	}
    	*/

    }

    @Override
    public Iterator<KVPair> readRange(char[] startKey, char[] endKey) {
    	ArrayList<KVPair> temp_pairs = new ArrayList<KVPair>();
    	String started = new String(startKey);
    	String ending = new String(endKey);
    	//If the start key is greater than the endkey
    	if (started.compareTo(ending) > 0)
    		return null;
    	for (String k : this.krazy_keys.keySet()) {
    		char[] next_kvp_e2 = this.krazy_keys.get(k);
    		//System.out.println(next_kvp.element1.toString().compareTo(ending));
    		//String next_kvp_e1 = new String(next_kvp.element1);
    		KVPair next_kvp = new KVPair(k.toCharArray(), next_kvp_e2);
    		boolean in_range = k.compareTo(started) >=0 && k.compareTo(ending)<=0;
    		if (in_range) {
    			temp_pairs.add(next_kvp);
    		}
    	}
    	//System.out.println("First ele:"+new String(temp_pairs.get(0).element1)+new String(temp_pairs.get(4).element1));
    	return temp_pairs.iterator();
    }

    @Override
    public void beginTx() {
    	// locks on this/ the whole thread?
    	System.out.println("Txn Begins!");
    }
    

    @Override
    public void commit() {
    	//Put everything onto disk (right now, read from reverse)
    	try {
    		//create new temp file of actual
    		//iterate over actual path
    		//check each value if it is in hashmap (if not, check temp file)
    		//if not in either, then write from actual file
    		//if in one of those, write most up to date
    		//copy file down
    		String temp_p = "/home/kbrandes/simpleKV/src/main/java/core/core/TEMP.txt";
    		File tfile = new File(temp_p);
    		System.out.println("pre first done");
    		BufferedWriter bw = new BufferedWriter(new FileWriter(tfile));
    		
			System.out.println("first done");
			BufferedReader act_br = new BufferedReader(new FileReader(this.actual_file));
			System.out.println("second done");
			String a;
			while ((a=act_br.readLine())!= null) {
				String[] arrofpair = a.split(" , ");
				//check if its in hashmap first
				if (this.krazy_keys.containsKey(arrofpair[0])) {
					System.out.println("here");
					//best value -- break from loop
					String val = new String(this.krazy_keys.get(arrofpair[0]));
					System.out.println("Hashmap value: "+val);
					String line = arrofpair[0]+" , "+val;
					bw.write(line);
					bw.newLine();
				} else {
					//check if it is in temp file
					BufferedReader temp_br = new BufferedReader(new FileReader(this.temp_file));
					System.out.println("temp read done");
					String t; boolean done = false;
					while ((t=temp_br.readLine()) != null && !done) {
						if (t.startsWith(arrofpair[0]+" , ")) {
							//found matching line, write to new file
							bw.write(t);
							bw.newLine();
							done = true;
						}
					}
					temp_br.close();
					if (!done) {
						//Not in hashmap or temp file, so just write same val as actual_file
						bw.write(a);
						bw.newLine();
					}
				}
				
			}
			System.out.println("Finished first thing");
			act_br.close();
			bw.close();
			this.actual_file.delete();
			File afile = new File(this.pathfile);
    		BufferedWriter bw_a = new BufferedWriter(new FileWriter(afile));
    		BufferedReader new_temp_br = new BufferedReader(new FileReader(tfile));
    		String s;
    		while ((s = new_temp_br.readLine()) != null) {
    			bw_a.write(s);
    			System.out.println("pre newline");
    			bw_a.newLine();
    			System.out.println("post newline");
    		}
    		bw_a.close();
    		new_temp_br.close();
    	}catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Commit - file not found");
		}
    	
		
    	//Make new temporary file
    	this.tid++;
    	this.temp_path = "/home/kbrandes/simpleKV/src/main/java/core/core/transaction"+this.tid+".txt";
		File tfile = new File(this.temp_path);
		this.temp_file = tfile;
    }

}
    
    
/*
 * 			
	    	String s; 

	    	//Temp file to disk:
			while ((s = br.readLine()) != null) {
				System.out.println("temp"+s);
				String[] arrofpair = s.split(" , ");
				BufferedReader br2 = new BufferedReader(new FileReader(this.actual_file));
				String s2;
				int line_count = 0;
				boolean done = false;
				System.out.println("before reading ");
				while((s2=br2.readLine())!=null && !done) {
					System.out.println("actual "+s2);
					line_count+= s2.getBytes().length;
					if (s2.startsWith(arrofpair[0]+" , ")) {
						System.out.println(s2+ " and "+s);
						RandomAccessFile f = new RandomAccessFile(this.actual_file, "rw");
						f.seek(line_count); // to the beginning
						f.writeChars(s);
						f.close();
						done = true;
					}
				}
				br2.close();
				BufferedWriter bw = new BufferedWriter(new FileWriter(this.actual_file));
		    	
				if (!done) {
					bw.write(s);
				}

				bw.close();
			}
			br.close();
			//Hashmap to disk:
			for (String k : this.krazy_keys.keySet()) {
				String v = this.krazy_keys.get(k).toString();
				String temp = k+" , "+v;
				BufferedReader br2 = new BufferedReader(new FileReader(this.actual_file));
				String s2;
				int line_count = 0;
				boolean done = false;
				while((s2=br2.readLine())!=null && !done) {
					line_count++;
					if (s2.startsWith(k+" , ")) {
						RandomAccessFile f = new RandomAccessFile(this.actual_file, "rw");
						f.seek(line_count); // to the beginning
						f.write(temp.getBytes());
						f.close();
						done = true;
					}
				}
				br2.close();
				BufferedWriter bw = new BufferedWriter(new FileWriter(this.actual_file));
		    	
				if (!done) {
					bw.write(temp);
				}
				bw.close();
			}
    	} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Commit - file not found");
		}*/
