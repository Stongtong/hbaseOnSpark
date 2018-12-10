package com.hxy.source

import com.google.protobuf.AbstractMessageLite

/**
  * @author tongtong
  *         descriptor ****
  *         create time 2018/11/26
  */
trait DataSource {
  def receive()
}
