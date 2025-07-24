/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package name.zicat.astatine.streaming.sql.runtime.test.map;

import name.zicat.astatine.streaming.sql.parser.function.FunctionFactory;
import name.zicat.astatine.streaming.sql.parser.test.transform.TransformFactoryTestBase;
import name.zicat.astatine.streaming.sql.parser.transform.MapTransformFactory;
import name.zicat.astatine.streaming.sql.parser.transform.TransformFactory;
import name.zicat.astatine.streaming.sql.runtime.map.Row2PojoMapFunctionFactory;
import name.zicat.astatine.streaming.sql.runtime.map.Row2PojoProperty;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/** Row2PojoMapFunctionFactoryTest. */
public class Row2PojoMapFunctionFactoryTest extends TransformFactoryTestBase {

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void test() throws Exception {

    final var configuration = new Configuration();
    configuration.set(
        FunctionFactory.OPTION_FUNCTION_IDENTITY, Row2PojoMapFunctionFactory.IDENTITY);
    configuration.set(Row2PojoMapFunctionFactory.OPTION_MAPPING_CLASS_NAME, MyPojo.class.getName());

    final var context = createContext(configuration);

    final var rowType =
        Types.ROW_NAMED(
            new String[] {
              "testByte",
              "testBool",
              "testShort",
              "testInt",
              "testLong",
              "testFloat",
              "testDouble",
              "testChar",
              "testString",
              "testWrapBool",
              "testWrapByte",
              "testWrapShort",
              "testWrapInt",
              "testWrapLong",
              "testWrapFloat",
              "testWrapDouble",
              "testWrapChar",
              "testNotExistsInPojo",
              "test_annotation_property",
              "testByteArray"
            },
            Types.BYTE,
            Types.BOOLEAN,
            Types.SHORT,
            Types.INT,
            Types.LONG,
            Types.FLOAT,
            Types.DOUBLE,
            Types.CHAR,
            Types.STRING,
            Types.BOOLEAN,
            Types.BYTE,
            Types.SHORT,
            Types.INT,
            Types.LONG,
            Types.FLOAT,
            Types.DOUBLE,
            Types.CHAR,
            Types.STRING,
            Types.STRING,
            TypeInformation.of(byte[].class));

    final var row = new Row(20);
    row.setField(0, (byte) 1);
    row.setField(1, true);
    row.setField(2, (short) 2);
    row.setField(3, 3);
    row.setField(4, 4L);
    row.setField(5, 5f);
    row.setField(6, 6d);
    row.setField(7, 'c');
    row.setField(8, "str");
    row.setField(9, true);
    row.setField(10, null);
    row.setField(11, (short) 11);
    row.setField(12, 12);
    row.setField(13, 13L);
    row.setField(14, 14f);
    row.setField(15, 15d);
    row.setField(16, 'd');
    row.setField(17, "other");
    row.setField(18, "test_annotation_property");
    row.setField(19, "test_bytes".getBytes());

    final var factory =
        TransformFactory.findFactory(MapTransformFactory.IDENTITY).cast(MapTransformFactory.class);
    execAndAssert(
        factory.transform(context, env.fromCollection(Collections.singleton(row), rowType)),
        (AssertHandler)
            result -> {
              Assert.assertEquals(1, result.size());
              final var pojo = (MyPojo) result.get(0);
              Assert.assertEquals((byte) 1, pojo.getTestByte());
              Assert.assertTrue(pojo.isTestBool());
              Assert.assertEquals((short) 2, pojo.getTestShort());
              Assert.assertEquals(3, pojo.getTestInt());
              Assert.assertEquals(4L, pojo.getTestLong());
              Assert.assertEquals(5f, pojo.getTestFloat(), 0);
              Assert.assertEquals(6d, pojo.getTestDouble(), 0);
              Assert.assertEquals('c', pojo.getTestChar());
              Assert.assertEquals("str", pojo.getTestString());
              Assert.assertTrue(pojo.getTestWrapBool());
              Assert.assertNull(pojo.getTestWrapByte());
              Assert.assertEquals((short) 11, pojo.getTestWrapShort().shortValue());
              Assert.assertEquals(12, pojo.getTestWrapInt().intValue());
              Assert.assertEquals(13L, pojo.getTestWrapLong().longValue());
              Assert.assertEquals(14f, pojo.getTestWrapFloat(), 0);
              Assert.assertEquals(15d, pojo.getTestWrapDouble(), 0);
              Assert.assertEquals('d', pojo.getTestWrapChar().charValue());
              Assert.assertEquals("test_annotation_property", pojo.getTestAnnotationProperty());
              Assert.assertEquals("test_bytes", new String(pojo.getTestByteArray()));
              Assert.assertNull(pojo.getTestNotExistsInRow());
            });
  }

  /** MyPojoBase. */
  public static class MyPojoBase {
    private boolean testBool;
    private byte testByte;
    private short testShort;
    private int testInt;
    private long testLong;
    private float testFloat;
    private double testDouble;
    private char testChar;

    public MyPojoBase() {}

    public MyPojoBase(
        boolean testBool,
        byte testByte,
        short testShort,
        int testInt,
        long testLong,
        float testFloat,
        double testDouble,
        char testChar) {
      this.testBool = testBool;
      this.testByte = testByte;
      this.testShort = testShort;
      this.testInt = testInt;
      this.testLong = testLong;
      this.testFloat = testFloat;
      this.testDouble = testDouble;
      this.testChar = testChar;
    }

    public boolean isTestBool() {
      return testBool;
    }

    public void setTestBool(boolean testBool) {
      this.testBool = testBool;
    }

    public byte getTestByte() {
      return testByte;
    }

    public void setTestByte(byte testByte) {
      this.testByte = testByte;
    }

    public short getTestShort() {
      return testShort;
    }

    public void setTestShort(short testShort) {
      this.testShort = testShort;
    }

    public int getTestInt() {
      return testInt;
    }

    public void setTestInt(int testInt) {
      this.testInt = testInt;
    }

    public long getTestLong() {
      return testLong;
    }

    public void setTestLong(long testLong) {
      this.testLong = testLong;
    }

    public float getTestFloat() {
      return testFloat;
    }

    public void setTestFloat(float testFloat) {
      this.testFloat = testFloat;
    }

    public double getTestDouble() {
      return testDouble;
    }

    public void setTestDouble(double testDouble) {
      this.testDouble = testDouble;
    }

    public char getTestChar() {
      return testChar;
    }

    public void setTestChar(char testChar) {
      this.testChar = testChar;
    }
  }

  /** MyPojo. */
  public static class MyPojo extends MyPojoBase {

    private String testString;
    private String testNotExistsInRow;

    private Boolean testWrapBool;
    private Byte testWrapByte;
    private Short testWrapShort;
    private Integer testWrapInt;
    private Long testWrapLong;
    private Float testWrapFloat;
    private Double testWrapDouble;
    private Character testWrapChar;

    @Row2PojoProperty("test_annotation_property")
    private String testAnnotationProperty;

    private byte[] testByteArray;

    public MyPojo() {}

    public MyPojo(
        boolean testBool,
        byte testByte,
        short testShort,
        int testInt,
        long testLong,
        float testFloat,
        double testDouble,
        char testChar,
        String testString,
        String testNotExistsInRow,
        Boolean testWrapBool,
        Byte testWrapByte,
        Short testWrapShort,
        Integer testWrapInt,
        Long testWrapLong,
        Float testWrapFloat,
        Double testWrapDouble,
        Character testWrapChar,
        String testAnnotationProperty) {
      super(testBool, testByte, testShort, testInt, testLong, testFloat, testDouble, testChar);
      this.testString = testString;
      this.testNotExistsInRow = testNotExistsInRow;
      this.testWrapBool = testWrapBool;
      this.testWrapByte = testWrapByte;
      this.testWrapShort = testWrapShort;
      this.testWrapInt = testWrapInt;
      this.testWrapLong = testWrapLong;
      this.testWrapFloat = testWrapFloat;
      this.testWrapDouble = testWrapDouble;
      this.testWrapChar = testWrapChar;
      this.testAnnotationProperty = testAnnotationProperty;
    }

    public String getTestString() {
      return testString;
    }

    public void setTestString(String testString) {
      this.testString = testString;
    }

    public String getTestNotExistsInRow() {
      return testNotExistsInRow;
    }

    public void setTestNotExistsInRow(String testNotExistsInRow) {
      this.testNotExistsInRow = testNotExistsInRow;
    }

    public Boolean getTestWrapBool() {
      return testWrapBool;
    }

    public void setTestWrapBool(Boolean testWrapBool) {
      this.testWrapBool = testWrapBool;
    }

    public Byte getTestWrapByte() {
      return testWrapByte;
    }

    public void setTestWrapByte(Byte testWrapByte) {
      this.testWrapByte = testWrapByte;
    }

    public Short getTestWrapShort() {
      return testWrapShort;
    }

    public void setTestWrapShort(Short testWrapShort) {
      this.testWrapShort = testWrapShort;
    }

    public Integer getTestWrapInt() {
      return testWrapInt;
    }

    public void setTestWrapInt(Integer testWrapInt) {
      this.testWrapInt = testWrapInt;
    }

    public Long getTestWrapLong() {
      return testWrapLong;
    }

    public void setTestWrapLong(Long testWrapLong) {
      this.testWrapLong = testWrapLong;
    }

    public Float getTestWrapFloat() {
      return testWrapFloat;
    }

    public void setTestWrapFloat(Float testWrapFloat) {
      this.testWrapFloat = testWrapFloat;
    }

    public Double getTestWrapDouble() {
      return testWrapDouble;
    }

    public void setTestWrapDouble(Double testWrapDouble) {
      this.testWrapDouble = testWrapDouble;
    }

    public String getTestAnnotationProperty() {
      return testAnnotationProperty;
    }

    public void setTestAnnotationProperty(String testAnnotationProperty) {
      this.testAnnotationProperty = testAnnotationProperty;
    }

    public Character getTestWrapChar() {
      return testWrapChar;
    }

    public void setTestWrapChar(Character testWrapChar) {
      this.testWrapChar = testWrapChar;
    }

    public byte[] getTestByteArray() {
      return testByteArray;
    }

    public void setTestByteArray(byte[] testByteArray) {
      this.testByteArray = testByteArray;
    }
  }
}
