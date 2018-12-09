package core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;
import java.io.*;
import java.util.*;

public class simpleKVTest  {

 
  /**
   * Unit test for Join.getTupleDesc()
   */
	
  @Test public void testConstructorKVTest() {
	  SimpleKV my_kv = new SimpleKV();
	  
  }
  @Test public void testfileRead() {
	  //SimpleKV my_kv = new SimpleKV("./test.txt");
	  //System.out.println(my_kv.krazy_keys[0]);
  }
  
  @Test public void testCompareKVPair() {
	  String t = "test";
	  String b = "best";
	  String t2 = "tqst";
	  KVPair kvp1 = new KVPair(t.toCharArray(), "v2".toCharArray());
	  KVPair kvp2= new KVPair(b.toCharArray(), "v1".toCharArray());
	  KVPair kvp3 = new KVPair(t2.toCharArray(), "v3".toCharArray());
	  
	  ArrayList<KVPair> my_kvps = new ArrayList<KVPair>();
	  my_kvps.add(kvp1);
	  my_kvps.add(kvp2);
	  my_kvps.add(kvp3);
	  System.out.println(new String (my_kvps.get(0).element1));
	  Collections.sort(my_kvps);
	  System.out.println(new String (my_kvps.get(0).element1));
	  System.out.println(new String (my_kvps.get(1).element1));
	  Iterator<KVPair> kv_it = my_kvps.iterator(); 
	  int count = 1;
	  char[] current = kv_it.next().element2;
	  System.out.println("size"+my_kvps.size());
	  while (current != null) {
		  String temp = "v"+count;
		  System.out.println("Current kvp:"+new String(current));
		  //assertEquals(new String(current), temp);
		  count++;
		  if (kv_it.hasNext()) {
			  current = kv_it.next().element2;  
		  }
		  else {
			  current = null;
		  }
	  }
	  assertEquals(count-1, 3);

	  
  }
  
  @Test public void writeKVTest() throws Exception {
	  SimpleKV my_kv = new SimpleKV();
	  
	  //Test first write
	  String k = "test"; 
	  String v = "value";
	  my_kv.write(k.toCharArray(), v.toCharArray());
	  assertEquals(1,my_kv.get_size());
	  
	  //Test multiple writes
	  my_kv.write("t2".toCharArray(), "v2".toCharArray());
	  my_kv.write("t3".toCharArray(), "v3".toCharArray());
	  assertEquals(3, my_kv.get_size());
	  
	  //Test overwrite for same key, different value
	  my_kv.write(k.toCharArray(), "new_v".toCharArray());
	  assertEquals(3, my_kv.get_size());
	  
	  //test invalid (null) key or value inserts
  }
  
  @Test public void readKVTest() throws Exception {
	  SimpleKV my_kv = new SimpleKV();
	  my_kv.write("test".toCharArray(), "value".toCharArray());
	  assertEquals(1, my_kv.get_size());
	  
	  char[] output = my_kv.read("test".toCharArray());
	  //assertTrue(output.length != 0);
	  assertEquals(new String(output), "value");
  }
  
  @Test public void rangeKVTest() throws Exception {
	  SimpleKV my_kv = new SimpleKV();
	  my_kv.write("t1".toCharArray(), "v1".toCharArray());
	  my_kv.write("t2".toCharArray(), "v2".toCharArray());
	  my_kv.write("t3".toCharArray(), "v3".toCharArray());
	  my_kv.write("t4".toCharArray(), "v4".toCharArray());
	  my_kv.write("t5".toCharArray(), "v5".toCharArray());
	  my_kv.write("t6".toCharArray(), "v6".toCharArray());
	  
	  Iterator<KVPair> kv_it = my_kv.readRange("t1".toCharArray(), "t5".toCharArray());
	  assertTrue(kv_it != null);
	  int count = 1;
	  char[] current = kv_it.next().element2;
	  while (current != null) {
		  String temp = "v"+count;
		  System.out.println(new String(current));
		  //assertEquals(new String(current), temp);
		  if (kv_it.hasNext()) {
			  current = kv_it.next().element2; 
			  count++;
		  }
		  else
			  current = null;
	  }
	  assertEquals(count,5);
  }

  @Test public void loadWriteTest() throws Exception {
	  SimpleKV my_kv = new SimpleKV();
	  long startTime = System.currentTimeMillis();
	  for (int i = 0; i<100000; i++) {
		  String temp = "t"+i;
		  String tempval = "v"+i;
		  my_kv.write(temp.toCharArray(), tempval.toCharArray());
	  }
	  long endTime = System.currentTimeMillis();
	  long duration = endTime - startTime; 
	  System.out.println("Loaded 100,000 writes in "+ duration+" ms");
	  long throughput = (long) ((100000/duration)*(1/.001));
	  System.out.println("Throughput: "+throughput);
    
  }
  
  @Test public void loadReadTest() throws Exception {
	  SimpleKV my_kv = new SimpleKV();
	  long startTime = System.currentTimeMillis();
	  for (int i = 0; i<100000; i++) {
		  String temp = "t"+i;
		  String tempval = "v"+i;
		  my_kv.write(temp.toCharArray(), tempval.toCharArray());
	  }
	  long midTime = System.currentTimeMillis();
	  for (int i = 0; i<100000; i++) {
		  String temp = "t"+i;
		  char[] out = my_kv.read(temp.toCharArray());
	  }
	  long endTime = System.currentTimeMillis();
	  long d1 = endTime - midTime;
	  long d2 = midTime-startTime;
	  System.out.println("Loaded 100,000 reads in "+ d1+"ms (did the writes in "+d2+"ms)");
    
  }
  
  
  
  @Test public void loadReadWriteTest() throws Exception {
	  SimpleKV my_kv = new SimpleKV();
	  long startTime = System.currentTimeMillis();
	  for (int i = 0; i<100000; i++) {
		  String temp = "t"+i;
		  String tempval = "v"+i;
		  my_kv.write(temp.toCharArray(), tempval.toCharArray());
		  char[] out = my_kv.read(temp.toCharArray());
	  }
	  for (int i = 0; i< 1000; i++) {
		  String temp = "t"+i+25;
		  String newt = "n"+i;
		  char[] out2 = my_kv.read(temp.toCharArray());
		  my_kv.write(newt.toCharArray(), temp.toCharArray());
	  }
	  long endTime = System.currentTimeMillis();
	  long duration = endTime - startTime;
	  System.out.println("Load Test mix read/write(2*100,000+2*1000) in "+duration);
    
  }
  
  @Test public void loadRangeReadTest() throws Exception {
	  SimpleKV my_kv = new SimpleKV();
	  for (int i = 0; i<100000; i++) {
		  String temp = "t"+i;
		  String tempval = "v"+i;
		  my_kv.write(temp.toCharArray(), tempval.toCharArray());
		  char[] out = my_kv.read(temp.toCharArray());
	  }
	  long startTime = System.currentTimeMillis();
	  Iterator<KVPair> kv_it = my_kv.readRange("t1".toCharArray(), "t5".toCharArray());
	  assertTrue(kv_it != null);
	  int count = 1;
	  char[] current = kv_it.next().element2;
	  while (kv_it.hasNext()) {
		  String temp = "t"+count;
		  char[] out = my_kv.read(temp.toCharArray());
		  count++;
		  current = kv_it.next().element2;  
	  }
	  long endTime = System.currentTimeMillis();
	  long duration = endTime - startTime;
	  System.out.println("Load range test in"+ duration+" ms");
    
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(simpleKVTest.class);
  }
}