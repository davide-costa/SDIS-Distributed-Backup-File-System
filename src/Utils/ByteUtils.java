package Utils;

/**********************************************************************************
 *
 * Copyright (c) 2003, 2004 The Regents of the University of Michigan, Trustees of Indiana University,
 *                  Board of Trustees of the Leland Stanford, Jr., University, and The MIT Corporation
 *
 * Licensed under the Educational Community License Version 1.0 (the "License");
 * By obtaining, using and/or copying this Original Work, you agree that you have read,
 * understand, and will comply with the terms and conditions of the Educational Community License.
 * You may obtain a copy of the License at:
 *
 *      http://cvs.sakaiproject.org/licenses/license_1_0.html
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE
 * AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 **********************************************************************************/


/**
 * Byte utilities
 */
public class ByteUtils {


    /**
     * Returns the index in the source array where the first occurrence of the
     * specified byte pattern is found
     *
     * @param source Byte array to examine
     * @param match  Byte array to locate in <code>source</code>
     * @return Index of the first matching character (-1 if no match)
     */
    public static int indexOf(byte[] source, byte[] match) {
        for (int i = 0; i < source.length; i++) {
            if (startsWith(source, i, match)) {
                return i;
            }
        }
        return -1;
    }

    public static int indexOf(byte[] source, String matchString) {
        return indexOf(source, matchString.getBytes());
    }

    /**
     * Returns the index in the source array where the last occurrence of the
     * specified text (a String, converted to a byte array) is found
     *
     * @param source      Byte array to examine
     * @param matchString String to locate in <code>source</code>
     * @return Index of the first matching character (-1 if no match)
     */
    public static int lastIndexOf(byte[] source, String matchString) {
        return lastIndexOf(source, matchString.getBytes());
    }

    /**
     * Returns the index in the source array where the last occurrence of the
     * specified byte pattern is found
     *
     * @param source Byte array to examine
     * @param match  Byte array to locate in <code>source</code>
     * @return Index of the last matching character (-1 if no match)
     */
    public static int lastIndexOf(byte[] source, byte[] match) {

        if (source.length < match.length) {
            return -1;
        }

        for (int i = (source.length - match.length); i >= 0; i--) {
            if (startsWith(source, i, match)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Does this byte array begin with match array content?
     *
     * @param source Byte array to examine
     * @param match  Byte array to locate in <code>source</code>
     * @return true If the starting bytes are equal
     */
    public static boolean startsWith(byte[] source, byte[] match) {
        return startsWith(source, 0, match);
    }

    /**
     * Does this byte array begin with match array content?
     *
     * @param source Byte array to examine
     * @param offset An offset into the <code>source</code> array
     * @param match  Byte array to locate in <code>source</code>
     * @return true If the starting bytes are equal
     */
    public static boolean startsWith(byte[] source, int offset, byte[] match) {

        if (match.length > (source.length - offset)) {
            return false;
        }

        for (int i = 0; i < match.length; i++) {
            if (source[offset + i] != match[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Does the source array equal the match array?
     *
     * @param source Byte array to examine
     * @param match  Byte array to locate in <code>source</code>
     * @return true If the two arrays are equal
     */
    public static boolean equals(byte[] source, byte[] match) {

        if (match.length != source.length) {
            return false;
        }
        return startsWith(source, 0, match);
    }

    /**
     * Copies bytes from the source byte array to the destination array
     *
     * @param source      The source array
     * @param srcBegin    Index of the first source byte to copy
     * @param srcEnd      Index after the last source byte to copy
     * @param destination The destination array
     * @param dstBegin    The starting offset in the destination array
     */
    public static void getBytes(byte[] source, int srcBegin, int srcEnd, byte[] destination,
                                int dstBegin) {
        System.arraycopy(source, srcBegin, destination, dstBegin, srcEnd - srcBegin);
    }

    /**
     * Return a new byte array containing a sub-portion of the source array
     *
     * @param srcBegin The beginning index (inclusive)
     * @param srcEnd   The ending index (exclusive)
     * @return The new, populated byte array
     */
    public static byte[] subbytes(byte[] source, int srcBegin, int srcEnd) {
        byte destination[];

        destination = new byte[srcEnd - srcBegin];
        getBytes(source, srcBegin, srcEnd, destination, 0);

        return destination;
    }

    /**
     * Return a new byte array containing a sub-portion of the source array
     *
     * @param srcBegin The beginning index (inclusive)
     * @return The new, populated byte array
     */
    public static byte[] subbytes(byte[] source, int srcBegin) {
        return subbytes(source, srcBegin, source.length);
    }
}
