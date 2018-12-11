package core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import junit.framework.JUnit4TestAdapter;

//import org.junit.Before;
import org.junit.Test;
import java.io.*;
import java.util.*;
public class simpleKVTest  {

 
  /**
   * Unit test for Join.getTupleDesc()
   */
	
  @Test public void testConstructorKVTest() {
	  SimpleKV my_kv = new SimpleKV();
	  SimpleKV my_kv2 = my_kv.initAndMakeStore("/transaction0");
	  System.out.println(my_kv2.get_actual_path());
  }
  
  @Test public void getThroughput() {
	  SimpleKV my_kv2 = new SimpleKV();
	  SimpleKV my_kv = my_kv2.initAndMakeStore("/small_test");
	  long starttTime = System.currentTimeMillis();
	  for (int i = 0; i<1000; i++) {
		  String temp = "t"+i;
		  String tempval = "v"+i;
		  my_kv.write(temp.toCharArray(), tempval.toCharArray());
	  }
	  long endTimeWrite = System.currentTimeMillis();
	  long small_write_throughput = (long) (1000/(endTimeWrite - starttTime)*(1/.001));
	  System.out.println("Small writes: "+small_write_throughput);
	  long startRead = System.currentTimeMillis();
	  for (int i = 0; i<1000; i++) {
		  String temp = "t"+i;
		  my_kv.read(temp.toCharArray());
	  } 
	  long endRead = System.currentTimeMillis();
	  System.out.println(endRead);
	  long small_read_throughput = (long) (1000/(endRead - startRead)*(1/.001));
	  System.out.println("Small reads: "+small_read_throughput);
	  SimpleKV my_kv3 = new SimpleKV();
	  SimpleKV my_kv4 = my_kv3.initAndMakeStore("/big_test");
	  long starttTime2 = System.currentTimeMillis();
	  for (int i = 0; i<100000; i++) {
		  String temp = "t"+i;
		  String tempval = "v"+i;
		  my_kv4.write(temp.toCharArray(), tempval.toCharArray());
	  }
	  long endTimeWrite2 = System.currentTimeMillis();
	  long big_write_throughput = (long) (100000/(endTimeWrite2 - starttTime2)*(1/.001));
	  System.out.println("Big writes: "+big_write_throughput);
	  long startRead2 = System.currentTimeMillis();
	  for (int i = 0; i<10000; i++) {
		  String temp = "t"+i;
		  my_kv4.read(temp.toCharArray());
	  } 
	  long endRead2 = System.currentTimeMillis();
	  System.out.println(startRead2);
	  System.out.println(endRead2);
	  long big_read_throughput = (long) (10000/(endRead2 - startRead2)*(1/.001));
	  System.out.println("big reads: "+big_read_throughput);

  }
  
  @Test public void nonMemReadRangeTest() {
	  SimpleKV my_kv2 = new SimpleKV();
	  long prestart = Runtime.getRuntime().freeMemory();
	  SimpleKV my_kv = my_kv2.initAndMakeStore("/teststore");
	  String start = "t"+6374103;
	  String end = "t"+6384113;
	  long started = Runtime.getRuntime().freeMemory();
	  Iterator<KVPair> it = my_kv.readRange(start.toCharArray(), end.toCharArray());
	  long mid = Runtime.getRuntime().freeMemory();
	  int count = 0;
	  KVPair current = null;
	  while (it.hasNext()) {
		  current = it.next();
		  //System.out.println("Current: "+new String(current.element1));
		  count++;
	  }
	  System.out.println("matches: "+count);
	  long ended = Runtime.getRuntime().freeMemory();
	  System.out.println("Mem: "+prestart+" "+started+ " "+mid+" "+ended);
  }
  
  @Test public void multiTxnTest() throws Exception {
	  SimpleKV my_kv2 = new SimpleKV();
	  SimpleKV my_kv = my_kv2.initAndMakeStore("/testmulti");
	  my_kv.beginTx();
	  long start = Runtime.getRuntime().freeMemory();
	  System.out.println("Free "+start);
	  for (int i = 0; i<10000; i++) {
		  String temp = "t"+i;
		  String tempval = "v"+i;
		  my_kv.write(temp.toCharArray(), tempval.toCharArray());
	  }
	  System.out.println("Free txn1 post "+Runtime.getRuntime().freeMemory());
	  long out = start - Runtime.getRuntime().freeMemory();
	  System.out.println("All txn1: "+out);
	  my_kv.commit();
	  
	  my_kv.beginTx();
	  long start1 = Runtime.getRuntime().freeMemory();
	  System.out.println("Free txn2: "+start1);
	  for (int i = 0; i<10000; i++) {
		  String temp = "a"+i;
		  String tempval = "c"+i;
		  my_kv.write(temp.toCharArray(), tempval.toCharArray());
	  }
	  System.out.println("Free txn2 post"+Runtime.getRuntime().freeMemory());
	  long out2 = start - Runtime.getRuntime().freeMemory();
	  System.out.println("All txn2: "+out2);
	  my_kv.commit();
	  
	  my_kv.beginTx();
	  long start2 = Runtime.getRuntime().freeMemory();
	  System.out.println("Free txn3:"+start2);
	  for (int i = 0; i<10000; i++) {
		  String temp = "t"+i;
		  String tempval = "v"+666+i;
		  my_kv.write(temp.toCharArray(), tempval.toCharArray());
	  }
	  System.out.println("Free txn3 post"+Runtime.getRuntime().freeMemory());
	  long out3 = start2 - Runtime.getRuntime().freeMemory();
	  System.out.println("All txn3: "+out3);
	  my_kv.commit();
  }
  
  @Test public void nonmemWriteTest() throws Exception {
	  SimpleKV my_kv2 = new SimpleKV();
	  SimpleKV my_kv = my_kv2.initAndMakeStore("/teststore");
	  my_kv.beginTx();
	  long startTime = System.currentTimeMillis();
	  long start = Runtime.getRuntime().freeMemory();
	  System.out.println("Free "+start);
	  System.out.println(Runtime.getRuntime().totalMemory());
	  for (int i = 0; i<50000000; i++) {
		  String temp = "t"+i;
		  String tempval = "v"+i;
		  my_kv.write(temp.toCharArray(), tempval.toCharArray());
	  }
	  String t = "v"+100000;
	  System.out.println(t.toCharArray().length);
	  System.out.println("Free "+Runtime.getRuntime().freeMemory());
	  System.out.println("Total "+Runtime.getRuntime().totalMemory());
	  long out = start - Runtime.getRuntime().freeMemory();
	  System.out.println("All: "+out);
	  long endTime = System.currentTimeMillis();
	  long duration = endTime - startTime; 
	  System.out.println("Loaded 100,000 writes in "+ duration+" ms");
	  long throughput = (long) ((100000/duration)*(1/.001));
	  System.out.println("Throughput: "+throughput);
	  my_kv.commit();
  }
  
  @Test public void testfileRead() {
	  SimpleKV my_kv = new SimpleKV();
	  SimpleKV my_kv2 = my_kv.initAndMakeStore("/read_test");
	  //assertEquals(my_kv2.get_size(),2);
	  System.out.println(my_kv2.read((new String("hello")).toCharArray()));
  }
  
  @Test public void testNonMemRead() {
	  SimpleKV my_kv = new SimpleKV();
	  SimpleKV my_kv2 = my_kv.initAndMakeStore("/read_test");
	  System.out.println(my_kv2.get_path());
	  //assertEquals(my_kv2.get_size(),2);
	  //System.out.println(my_kv2.read((new String("hello")).toCharArray()));
	  char[] readout = my_kv2.read(new String("sup").toCharArray()); 
	  //System.out.println(new String(readout));
	  //assertEquals(new String(readout), "dude");
	  char [] readmem = my_kv2.read(new String("hello").toCharArray());
	  //assertEquals(new String(readmem), "hi");
  }
  
  @Test public void testNonMemWrite() {
	  SimpleKV my_kv2 = new SimpleKV();
	  SimpleKV my_kv= my_kv2.initAndMakeStore("/test");
	  
	  //assertEquals(my_kv2.get_size(),2);
	  //System.out.println(my_kv2.read((new String("hello")).toCharArray()));
	  my_kv.write("t1".toCharArray(), "v1".toCharArray());
	  my_kv.write("t2".toCharArray(), "v2".toCharArray());
	  my_kv.write("t3".toCharArray(), "v3".toCharArray());
	  my_kv.write("t4".toCharArray(), "v4".toCharArray());
	  my_kv.write("t5".toCharArray(), "v5".toCharArray());
	  my_kv.write("t6".toCharArray(), "v6".toCharArray());
	  assertEquals(9, my_kv.get_size());
	  //go look at temp
  }
  @Test public void testNonMemRR() {
	  SimpleKV my_kv2 = new SimpleKV();
	  SimpleKV my_kv= my_kv2.initAndMakeStore("/test");
	  
	  //assertEquals(my_kv2.get_size(),2);
	  //System.out.println(my_kv2.read((new String("hello")).toCharArray()));
	  my_kv.write("t1".toCharArray(), "v1".toCharArray());
	  my_kv.write("t2".toCharArray(), "v2".toCharArray());
	  my_kv.write("t3".toCharArray(), "v3".toCharArray());
	  my_kv.write("t4".toCharArray(), "v4".toCharArray());
	  my_kv.write("t5".toCharArray(), "v5".toCharArray());
	  my_kv.write("t6".toCharArray(), "v6".toCharArray());
	  Iterator<KVPair> it = my_kv.readRange("sup".toCharArray(), "tu".toCharArray());
	  while (it.hasNext()){
		  System.out.println("Readrange test: "+new String(it.next().element1));
	  }
  }
  
  @Test public void testCommit() {
	  SimpleKV my_kv = new SimpleKV();
	  SimpleKV my_kv2 = my_kv.initAndMakeStore("/test");;
	  
	  //System.out.println(my_kv2.read((new String("hello")).toCharArray()));
	  //my_kv2.help_overwrite("/home/kbrandes/simpleKV/src/main/java/core/core/over_temp.txt");
	  //System.out.println("overwrite done");
	  my_kv2.commit();
	  //go check file on your own
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
//	  System.out.println(new String (my_kvps.get(0).element1));
	  Collections.sort(my_kvps);
//	  System.out.println(new String (my_kvps.get(0).element1));
//	  System.out.println(new String (my_kvps.get(1).element1));
	  Iterator<KVPair> kv_it = my_kvps.iterator(); 
	  int count = 1;
	  char[] current = kv_it.next().element2;
//	  System.out.println("size"+my_kvps.size());
	  while (current != null) {
		  String temp = "v"+count;
//		  System.out.println("Current kvp:"+new String(current));
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
//	  System.out.println("Loaded 100,000 reads in "+ d1+"ms (did the writes in "+d2+"ms)");
    
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
//	  System.out.println("Load Test mix read/write(2*100,000+2*1000) in "+duration);
    
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
//	  System.out.println("Load range test in"+ duration+" ms");
    
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(simpleKVTest.class);
  }
}  