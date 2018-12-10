package com.hxy.util

import java.io.InputStream
import java.util.Objects

import com.fasterxml.jackson.core.JsonParser.Feature
import com.fasterxml.jackson.databind.ObjectMapper
import com.hxy.json.{Config, TablesInfo}
import com.hxy.util.Json2Object.getClass

import scala.reflect.ClassTag

/**
  * @author tongtong
  *         descriptor **** parse json to object
  *         create time 2018/11/29
  */
//class Json2Object{
//  def parse[T <: GeneralConfig](path: String) = {
//    Objects.requireNonNull(path, "The file path is null.")
//    val mapper = new ObjectMapper
//    //解析器支持解析单引号
//    mapper.configure(Feature.ALLOW_SINGLE_QUOTES, true)
//    //解析器支持解析结束符
//    mapper.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
//    val stream: InputStream = getClass.getResourceAsStream(path)
//    val lines = scala.io.Source.fromInputStream(stream).getLines
//    val builder = StringBuilder.newBuilder
//    lines.foreach(line => builder append line)
//    val ru = scala.reflect.runtime.universe
//    mapper readValue(builder.toString(), asInstanceOf[T].getClass)
//  }
//}

object Json2Object {

  //  def apply: Json2Object = new Json2Object()

  def parse1[T](path: String) = {
    Objects.requireNonNull(path, "The file path is null.")
    val mapper = new ObjectMapper
    //解析器支持解析单引号
    mapper.configure(Feature.ALLOW_SINGLE_QUOTES, true)
    //解析器支持解析结束符
    mapper.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
    val stream: InputStream = getClass.getResourceAsStream(path)
    val lines = scala.io.Source.fromInputStream(stream).getLines
    val builder = StringBuilder.newBuilder
    lines.foreach(line => builder append line)
    val ru = scala.reflect.runtime.universe
    mapper readValue(builder.toString(), classOf[Config])
  }

  def parse[T](path: String) = {
    Objects.requireNonNull(path, "The file path is null.")
    val mapper = new ObjectMapper
    //解析器支持解析单引号
    mapper.configure(Feature.ALLOW_SINGLE_QUOTES, true)
    //解析器支持解析结束符
    mapper.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
    val stream: InputStream = getClass.getResourceAsStream(path)
    val lines = scala.io.Source.fromInputStream(stream).getLines
    val builder = StringBuilder.newBuilder
    lines.foreach(line => builder append line)
    val ru = scala.reflect.runtime.universe
    mapper readValue(builder.toString(), classOf[TablesInfo])
  }
}
