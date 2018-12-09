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
    	this.pathfile = null;
    	this.tid = 0;
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
    		try {
				BufferedReader br = new BufferedReader( new FileReader(file));
				String s; String t;
				while ((s = br.readLine()) != null) {
					//add it to the kv krazy_keys store
					t = br.readLine();
					this.krazy_keys.put(s, t.toCharArray());
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				System.out.println("No file found");
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
    	char[] next_kvp = this.krazy_keys.get(k_string);
    	return next_kvp;
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
    	this.temp_path = "~/transaction"+this.tid+".txt";
    	this.tid++; // locks on this/ the whole thread?
    	
    	
    	System.out.println("Done!");
    }

    @Override
    public void commit() {
    	if (this.temp_file.length() == 0) {
    		//only look at memory structure
    	}
    	else {
    		//copy file to replace current one
    	}
    	try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(this.temp_path));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	for (String k : this.krazy_keys.keySet()) {
    		
    	}
	System.out.println("Done!");
    }

}
