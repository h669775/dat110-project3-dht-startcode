package no.hvl.dat110.util;

/**
 * exercise/demo purpose in dat110
 * @author tdoy
 *
 */

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash { 
	
	
	public static BigInteger hashOf(String entity) {	
		
		BigInteger hashint = null;
        
        try {
            // Use MD5 hash algorithm
            MessageDigest md = MessageDigest.getInstance("MD5");
            // Compute the hash of the input 'entity'
            byte[] digest = md.digest(entity.getBytes());
            // Convert the hash into hex format
            String hex = toHex(digest);
            // Convert the hex into BigInteger
            hashint = new BigInteger(hex, 16);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        
        // Return the BigInteger
        return hashint;
    }
	
	public static BigInteger addressSize() {
        // Compute the address size = 2 ^ bitSize()
        return BigInteger.valueOf(2).pow(bitSize());
    }
    
    public static int bitSize() {
        // MD5 produces a 128-bit hash
        return 128;
    }
    
    public static String toHex(byte[] digest) {
        StringBuilder strbuilder = new StringBuilder();
        for(byte b : digest) {
            strbuilder.append(String.format("%02x", b & 0xff));
        }
        return strbuilder.toString();
    }
}
