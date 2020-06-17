package com.atguigu.temp

import java.util.Calendar

/**
 * @description: ${description}
 * @author: Davon
 * @created: 2020/06/17 08:35
 */
object Test {
  def main(args: Array[String]): Unit = {
    val map1 = Map(("hello", List(1)))
    val map2: Map[Int, Int] = null
    val i: Int = map1.getOrElse("hell", null).iterator.next()
    println(i)
  }

}
