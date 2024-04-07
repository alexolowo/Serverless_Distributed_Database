package shared;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {
    
    /**
     * MD5 Hash function used to place string keys into
     * the appropriate hash range in the KVServer ring.
     * 
     * MD5 produces a 128-bit hash, so the range is:
     * min : 0x00000000000000000000000000000000
     * max : 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
     * 
     * There is a ~10^(-29) probability of collision, so
     * no hash collision is dealt with
     * 
     * @param str to get hash of
     * @return BigInteger representing the hash
     */
    public static BigInteger hash(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(str.getBytes());
            byte[] digest = md.digest();

            return new BigInteger(1, digest);
        } catch (NoSuchAlgorithmException e) {
            return new BigInteger("-1");
        }
    }


    public static boolean inHashRange(String key, BigInteger low, BigInteger high) {
        BigInteger keyHash = Hash.hash(key);

        boolean aboveKRStart = 
            keyHash.compareTo(low) >= 0;
        boolean belowKREnd =
            keyHash.compareTo(high) <= 0;

        // if start of kr is larger than end of kr, it includes 0
        if (low.compareTo(high) > 0) { 
            return aboveKRStart || belowKREnd;
        }

        return aboveKRStart && belowKREnd;
    }
}
