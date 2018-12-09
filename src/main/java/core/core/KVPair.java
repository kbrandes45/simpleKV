package core;

public class KVPair implements Comparable<KVPair> {

    public char[] element1;
    public char[] element2;

    public KVPair(char[] element1, char[] element2) {
	this.element1 = element1;
	this.element2 = element2;
    }
    
    @Override
    public int compareTo(KVPair other_kvp) {
    	return -this.element1.toString().compareTo(other_kvp.element1.toString());

    	
    }

}
