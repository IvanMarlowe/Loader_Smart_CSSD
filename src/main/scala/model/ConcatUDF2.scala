///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package model
//
//import java.text.DecimalFormat
//import java.util.{HashMap, Locale, Map => JMap}
//
//import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.catalyst.expressions.codegen._
//import org.apache.spark.sql.catalyst.util.ArrayData
//import org.apache.spark.sql.types._
//import org.apache.spark.unsafe.types.{ByteArray, UTF8String}
//import org.apache.spark.sql.catalyst.expressions
//////////////////////////////////////////////////////////////////////////////////////////////////////
//// This file defines expressions for string operations.
//////////////////////////////////////////////////////////////////////////////////////////////////////
//
///**
// * An expression that concatenates multiple input strings or array of strings into a single string,
// * using a given separator (the first child).
// *
// * Returns null if the separator is null. Otherwise, concat_ws skips all null values.
// */
//
//
//
///**
// * A function that return the length of the given string or binary expression.
// */
//case class Length(child: Expression) extends UnaryExpression with ExpectsInputTypes {
//  override def dataType: DataType = IntegerType
//  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, BinaryType))
//
//  protected override def nullSafeEval(value: Any): Any = child.dataType match {
//    case StringType => value.asInstanceOf[UTF8String].numChars
//    case BinaryType => value.asInstanceOf[Array[Byte]].length
//  }
//
//  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
//    child.dataType match {
//      case StringType => defineCodeGen(ctx, ev, c => s"($c).numChars()")
//      case BinaryType => defineCodeGen(ctx, ev, c => s"($c).length")
//    }
//  }
//}
