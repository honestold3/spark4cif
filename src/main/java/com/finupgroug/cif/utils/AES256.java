package com.finupgroug.cif.utils;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

/**
 * Created by wq on 2016/12/30.
 */
public class AES256 {

    private SecretKeySpec secretKey;
    private byte[] key;

    public void setKey(String key) {
        MessageDigest sha = null;
        try {
            this.key = key.getBytes("UTF-8");
            sha = MessageDigest.getInstance("SHA-1");
            this.key = sha.digest(this.key);
            this.key = Arrays.copyOf(this.key, 16);
            secretKey = new SecretKeySpec(this.key, "AES");
        } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    public String getSecretKey() {
        return secretKey.toString();
    }

    public String getKey() {
        return new String(key);
    }

    public String encrypt(String string) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            //return Base64.getMimeEncoder().encodeToString(string.getBytes());

            try {
                return Base64.getMimeEncoder().encodeToString(cipher.doFinal(string.getBytes()));
            } catch (IllegalBlockSizeException e) {
                e.printStackTrace();
            } catch (BadPaddingException e) {
                e.printStackTrace();
            }

        } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException  e) {
            e.printStackTrace();
        }
        return null;
    }

    public String decrypt(String string) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            return new String(cipher.doFinal(Base64.getMimeDecoder().decode(string.getBytes())));
        } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException | IllegalBlockSizeException | BadPaddingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        AES256 aes = new AES256();

        aes.setKey("Secret Key");

        String enc = "";
        enc = aes.encrypt("ASC");

        System.out.println(enc);

        System.out.println(aes.decrypt(enc));
    }

}
