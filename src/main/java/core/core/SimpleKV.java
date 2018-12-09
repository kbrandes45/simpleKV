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
    		
    		this.temp_path = "/home/kbrandes/simpleKV/src/main/java/core/core/transaction"+this.tid+".txt";
    		File tfile = new File(this.temp_path);
    		this.temp_file = tfile;
    		try {
				BufferedReader br = new BufferedReader( new FileReader(kv.actual_file));
				BufferedWriter bw = new BufferedWriter( new FileWriter(this.temp_file));
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
    	//KVPair kvp = new KVPair(key, value);
    	String k_string = new String(key);
    	//String v_string = new String(value);
    	this.krazy_keys.put(k_string, value);
    	
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
    	//System.out.println("Written!");
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
        		BufferedReader br = new BufferedReader(new FileReader(this.temp_file));
        		String s;
				while ((s=br.readLine())!=null) {
					if (s.startsWith(k_string+" , ")) {
						String[] arrofpair = s.split(" , ");
						return arrofpair[1].toCharArray();
					}
				}
				System.out.println("Reached end of temp and hashmap, but no value:(");
				
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
			BufferedReader br = new BufferedReader(new FileReader(this.temp_file));
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
		}

    	//Make new temporary file
    	this.tid++;
    	this.temp_path = "/home/kbrandes/simpleKV/src/main/java/core/core/transaction"+this.tid+".txt";
		File tfile = new File(this.temp_path);
		this.temp_file = tfile;
    }

}
