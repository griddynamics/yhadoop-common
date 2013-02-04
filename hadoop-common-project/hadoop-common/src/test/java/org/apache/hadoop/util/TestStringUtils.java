/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import static org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix.long2String;
import static org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix.string2long;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.test.UnitTestcaseTimeLimit;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.junit.Test;

public class TestStringUtils extends UnitTestcaseTimeLimit {
  final private static String NULL_STR = null;
  final private static String EMPTY_STR = "";
  final private static String STR_WO_SPECIAL_CHARS = "AB";
  final private static String STR_WITH_COMMA = "A,B";
  final private static String ESCAPED_STR_WITH_COMMA = "A\\,B";
  final private static String STR_WITH_ESCAPE = "AB\\";
  final private static String ESCAPED_STR_WITH_ESCAPE = "AB\\\\";
  final private static String STR_WITH_BOTH2 = ",A\\,,B\\\\,";
  final private static String ESCAPED_STR_WITH_BOTH2 = 
    "\\,A\\\\\\,\\,B\\\\\\\\\\,";
  
  @Test
  public void testEscapeString() throws Exception {
    assertEquals(NULL_STR, StringUtils.escapeString(NULL_STR));
    assertEquals(EMPTY_STR, StringUtils.escapeString(EMPTY_STR));
    assertEquals(STR_WO_SPECIAL_CHARS,
        StringUtils.escapeString(STR_WO_SPECIAL_CHARS));
    assertEquals(ESCAPED_STR_WITH_COMMA,
        StringUtils.escapeString(STR_WITH_COMMA));
    assertEquals(ESCAPED_STR_WITH_ESCAPE,
        StringUtils.escapeString(STR_WITH_ESCAPE));
    assertEquals(ESCAPED_STR_WITH_BOTH2, 
        StringUtils.escapeString(STR_WITH_BOTH2));
  }
  
  @Test
  public void testSplit() throws Exception {
    assertEquals(NULL_STR, StringUtils.split(NULL_STR));
    String[] splits = StringUtils.split(EMPTY_STR);
    assertEquals(0, splits.length);
    splits = StringUtils.split(",,");
    assertEquals(0, splits.length);
    splits = StringUtils.split(STR_WO_SPECIAL_CHARS);
    assertEquals(1, splits.length);
    assertEquals(STR_WO_SPECIAL_CHARS, splits[0]);
    splits = StringUtils.split(STR_WITH_COMMA);
    assertEquals(2, splits.length);
    assertEquals("A", splits[0]);
    assertEquals("B", splits[1]);
    splits = StringUtils.split(ESCAPED_STR_WITH_COMMA);
    assertEquals(1, splits.length);
    assertEquals(ESCAPED_STR_WITH_COMMA, splits[0]);
    splits = StringUtils.split(STR_WITH_ESCAPE);
    assertEquals(1, splits.length);
    assertEquals(STR_WITH_ESCAPE, splits[0]);
    splits = StringUtils.split(STR_WITH_BOTH2);
    assertEquals(3, splits.length);
    assertEquals(EMPTY_STR, splits[0]);
    assertEquals("A\\,", splits[1]);
    assertEquals("B\\\\", splits[2]);
    splits = StringUtils.split(ESCAPED_STR_WITH_BOTH2);
    assertEquals(1, splits.length);
    assertEquals(ESCAPED_STR_WITH_BOTH2, splits[0]);    
  }
  
  @Test
  public void testSimpleSplit() throws Exception {
    final String[] TO_TEST = {
        "a/b/c",
        "a/b/c////",
        "///a/b/c",
        "",
        "/",
        "////"};
    for (String testSubject : TO_TEST) {
      assertArrayEquals("Testing '" + testSubject + "'",
        testSubject.split("/"),
        StringUtils.split(testSubject, '/'));
    }
  }

  @Test
  public void testUnescapeString() throws Exception {
    assertEquals(NULL_STR, StringUtils.unEscapeString(NULL_STR));
    assertEquals(EMPTY_STR, StringUtils.unEscapeString(EMPTY_STR));
    assertEquals(STR_WO_SPECIAL_CHARS,
        StringUtils.unEscapeString(STR_WO_SPECIAL_CHARS));
    try {
      StringUtils.unEscapeString(STR_WITH_COMMA);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertEquals(STR_WITH_COMMA,
        StringUtils.unEscapeString(ESCAPED_STR_WITH_COMMA));
    try {
      StringUtils.unEscapeString(STR_WITH_ESCAPE);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertEquals(STR_WITH_ESCAPE,
        StringUtils.unEscapeString(ESCAPED_STR_WITH_ESCAPE));
    try {
      StringUtils.unEscapeString(STR_WITH_BOTH2);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertEquals(STR_WITH_BOTH2,
        StringUtils.unEscapeString(ESCAPED_STR_WITH_BOTH2));
  }
  
  @Test
  public void testTraditionalBinaryPrefix() throws Exception {
    //test string2long(..)
    String[] symbol = {"k", "m", "g", "t", "p", "e"};
    long m = 1024;
    for(String s : symbol) {
      assertEquals(0, string2long(0 + s));
      assertEquals(m, string2long(1 + s));
      m *= 1024;
    }
    
    assertEquals(0L, string2long("0"));
    assertEquals(1024L, string2long("1k"));
    assertEquals(-1024L, string2long("-1k"));
    assertEquals(1259520L, string2long("1230K"));
    assertEquals(-1259520L, string2long("-1230K"));
    assertEquals(104857600L, string2long("100m"));
    assertEquals(-104857600L, string2long("-100M"));
    assertEquals(956703965184L, string2long("891g"));
    assertEquals(-956703965184L, string2long("-891G"));
    assertEquals(501377302265856L, string2long("456t"));
    assertEquals(-501377302265856L, string2long("-456T"));
    assertEquals(11258999068426240L, string2long("10p"));
    assertEquals(-11258999068426240L, string2long("-10P"));
    assertEquals(1152921504606846976L, string2long("1e"));
    assertEquals(-1152921504606846976L, string2long("-1E"));

    String tooLargeNumStr = "10e";
    try {
      string2long(tooLargeNumStr);
      fail("Test passed for a number " + tooLargeNumStr + " too large");
    } catch (IllegalArgumentException e) {
      assertEquals(tooLargeNumStr + " does not fit in a Long", e.getMessage());
    }

    String tooSmallNumStr = "-10e";
    try {
      string2long(tooSmallNumStr);
      fail("Test passed for a number " + tooSmallNumStr + " too small");
    } catch (IllegalArgumentException e) {
      assertEquals(tooSmallNumStr + " does not fit in a Long", e.getMessage());
    }

    String invalidFormatNumStr = "10kb";
    char invalidPrefix = 'b';
    try {
      string2long(invalidFormatNumStr);
      fail("Test passed for a number " + invalidFormatNumStr
          + " has invalid format");
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid size prefix '" + invalidPrefix + "' in '"
          + invalidFormatNumStr
          + "'. Allowed prefixes are k, m, g, t, p, e(case insensitive)",
          e.getMessage());
    }

    //test long2string(..)
    assertEquals("0", long2String(0, null, 2));
    for(int decimalPlace = 0; decimalPlace < 2; decimalPlace++) {
      for(int n = 1; n < TraditionalBinaryPrefix.KILO.value; n++) {
        assertEquals(n + "", long2String(n, null, decimalPlace));
        assertEquals(-n + "", long2String(-n, null, decimalPlace));
      }
      assertEquals("1 K", long2String(1L << 10, null, decimalPlace));
      assertEquals("-1 K", long2String(-1L << 10, null, decimalPlace));
    }

    assertEquals("8.00 E", long2String(Long.MAX_VALUE, null, 2));
    assertEquals("8.00 E", long2String(Long.MAX_VALUE - 1, null, 2));
    assertEquals("-8 E", long2String(Long.MIN_VALUE, null, 2));
    assertEquals("-8.00 E", long2String(Long.MIN_VALUE + 1, null, 2));

    final String[] zeros = {" ", ".0 ", ".00 "};
    for(int decimalPlace = 0; decimalPlace < zeros.length; decimalPlace++) {
      final String trailingZeros = zeros[decimalPlace]; 

      for(int e = 11; e < Long.SIZE - 1; e++) {
        final TraditionalBinaryPrefix p
            = TraditionalBinaryPrefix.values()[e/10 - 1]; 
  
        { // n = 2^e
          final long n = 1L << e;
          final String expected = (n/p.value) + " " + p.symbol;
          assertEquals("n=" + n, expected, long2String(n, null, 2));
        }
  
        { // n = 2^e + 1
          final long n = (1L << e) + 1;
          final String expected = (n/p.value) + trailingZeros + p.symbol;
          assertEquals("n=" + n, expected, long2String(n, null, decimalPlace));
        }
  
        { // n = 2^e - 1
          final long n = (1L << e) - 1;
          final String expected = ((n+1)/p.value) + trailingZeros + p.symbol;
          assertEquals("n=" + n, expected, long2String(n, null, decimalPlace));
        }
      }
    }

    assertEquals("1.50 K", long2String(3L << 9, null, 2));
    assertEquals("1.5 K", long2String(3L << 9, null, 1));
    assertEquals("1.50 M", long2String(3L << 19, null, 2));
    assertEquals("2 M", long2String(3L << 19, null, 0));
    assertEquals("3 G", long2String(3L << 30, null, 2));

    // test byteDesc(..)
    assertEquals("0 B", StringUtils.byteDesc(0));
    assertEquals("-100 B", StringUtils.byteDesc(-100));
    assertEquals("1 KB", StringUtils.byteDesc(1024));
    assertEquals("1.50 KB", StringUtils.byteDesc(3L << 9));
    assertEquals("1.50 MB", StringUtils.byteDesc(3L << 19));
    assertEquals("3 GB", StringUtils.byteDesc(3L << 30));
    
    // test formatPercent(..)
    assertEquals("10%", StringUtils.formatPercent(0.1, 0));
    assertEquals("10.0%", StringUtils.formatPercent(0.1, 1));
    assertEquals("10.00%", StringUtils.formatPercent(0.1, 2));

    assertEquals("1%", StringUtils.formatPercent(0.00543, 0));
    assertEquals("0.5%", StringUtils.formatPercent(0.00543, 1));
    assertEquals("0.54%", StringUtils.formatPercent(0.00543, 2));
    assertEquals("0.543%", StringUtils.formatPercent(0.00543, 3));
    assertEquals("0.5430%", StringUtils.formatPercent(0.00543, 4));
  }

  @Test
  public void testJoin() {
    List<String> s = new ArrayList<String>();
    s.add("a");
    s.add("b");
    s.add("c");
    assertEquals("", StringUtils.join(":", s.subList(0, 0)));
    assertEquals("a", StringUtils.join(":", s.subList(0, 1)));
    assertEquals("a:b", StringUtils.join(":", s.subList(0, 2)));
    assertEquals("a:b:c", StringUtils.join(":", s.subList(0, 3)));
  }
  
  @Test
  public void testGetTrimmedStrings() throws Exception {
    String compactDirList = "/spindle1/hdfs,/spindle2/hdfs,/spindle3/hdfs";
    String spacedDirList = "/spindle1/hdfs, /spindle2/hdfs, /spindle3/hdfs";
    String pathologicalDirList1 = " /spindle1/hdfs  ,  /spindle2/hdfs ,/spindle3/hdfs ";
    String pathologicalDirList2 = " /spindle1/hdfs  ,  /spindle2/hdfs ,/spindle3/hdfs , ";
    String emptyList1 = "";
    String emptyList2 = "   ";
    
    String[] expectedArray = {"/spindle1/hdfs", "/spindle2/hdfs", "/spindle3/hdfs"};
    String[] emptyArray = {};
    
    assertArrayEquals(expectedArray, StringUtils.getTrimmedStrings(compactDirList));
    assertArrayEquals(expectedArray, StringUtils.getTrimmedStrings(spacedDirList));
    assertArrayEquals(expectedArray, StringUtils.getTrimmedStrings(pathologicalDirList1));
    assertArrayEquals(expectedArray, StringUtils.getTrimmedStrings(pathologicalDirList2));
    
    assertArrayEquals(emptyArray, StringUtils.getTrimmedStrings(emptyList1));
    String[] estring = StringUtils.getTrimmedStrings(emptyList2);
    assertArrayEquals(emptyArray, estring);
  } 

  @Test
  public void testCamelize() {
    // common use cases
    assertEquals("Map", StringUtils.camelize("MAP"));
    assertEquals("JobSetup", StringUtils.camelize("JOB_SETUP"));
    assertEquals("SomeStuff", StringUtils.camelize("some_stuff"));

    // sanity checks for ascii alphabet against unexpected locale issues.
    assertEquals("Aa", StringUtils.camelize("aA"));
    assertEquals("Bb", StringUtils.camelize("bB"));
    assertEquals("Cc", StringUtils.camelize("cC"));
    assertEquals("Dd", StringUtils.camelize("dD"));
    assertEquals("Ee", StringUtils.camelize("eE"));
    assertEquals("Ff", StringUtils.camelize("fF"));
    assertEquals("Gg", StringUtils.camelize("gG"));
    assertEquals("Hh", StringUtils.camelize("hH"));
    assertEquals("Ii", StringUtils.camelize("iI"));
    assertEquals("Jj", StringUtils.camelize("jJ"));
    assertEquals("Kk", StringUtils.camelize("kK"));
    assertEquals("Ll", StringUtils.camelize("lL"));
    assertEquals("Mm", StringUtils.camelize("mM"));
    assertEquals("Nn", StringUtils.camelize("nN"));
    assertEquals("Oo", StringUtils.camelize("oO"));
    assertEquals("Pp", StringUtils.camelize("pP"));
    assertEquals("Qq", StringUtils.camelize("qQ"));
    assertEquals("Rr", StringUtils.camelize("rR"));
    assertEquals("Ss", StringUtils.camelize("sS"));
    assertEquals("Tt", StringUtils.camelize("tT"));
    assertEquals("Uu", StringUtils.camelize("uU"));
    assertEquals("Vv", StringUtils.camelize("vV"));
    assertEquals("Ww", StringUtils.camelize("wW"));
    assertEquals("Xx", StringUtils.camelize("xX"));
    assertEquals("Yy", StringUtils.camelize("yY"));
    assertEquals("Zz", StringUtils.camelize("zZ"));
  }
  
  @Test
  public void testStringToURI() {
    String[] str = new String[] { "file://" };
    try {
      StringUtils.stringToURI(str);
      fail("Ignoring URISyntaxException while creating URI from string file://");
    } catch (IllegalArgumentException iae) {
      assertEquals("Failed to create uri for file://", iae.getMessage());
    }
  }

  @Test
  public void testSimpleHostName() {
    assertEquals("Should return hostname when FQDN is specified",
            "hadoop01",
            StringUtils.simpleHostname("hadoop01.domain.com"));
    assertEquals("Should return hostname when only hostname is specified",
            "hadoop01",
            StringUtils.simpleHostname("hadoop01"));
    assertEquals("Should not truncate when IP address is passed",
            "10.10.5.68",
            StringUtils.simpleHostname("10.10.5.68"));
  }

  // Benchmark for StringUtils split
  public static void main(String []args) {
    final String TO_SPLIT = "foo,bar,baz,blah,blah";
    for (boolean useOurs : new boolean[] { false, true }) {
      for (int outer=0; outer < 10; outer++) {
        long st = System.nanoTime();
        int components = 0;
        for (int inner=0; inner < 1000000; inner++) {
          String[] res;
          if (useOurs) {
            res = StringUtils.split(TO_SPLIT, ',');
          } else {
            res = TO_SPLIT.split(",");
          }
           // be sure to use res, otherwise might be optimized out
          components += res.length;
        }
        long et = System.nanoTime();
        if (outer > 3) {
          System.out.println( (useOurs ? "StringUtils impl" : "Java impl")
              + " #" + outer + ":" + (et - st)/1000000 + "ms, components="
              + components  );
        }
      }
    }
  }
}
