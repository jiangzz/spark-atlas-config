package com.jd.commons

object SQLQueryContext {
  private[this] val sqlQuery = new ThreadLocal[String]
  def get(): String = sqlQuery.get
  def set(s: String): Unit = sqlQuery.set(s)
}
