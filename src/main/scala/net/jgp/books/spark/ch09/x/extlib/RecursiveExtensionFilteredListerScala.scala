package net.jgp.books.spark.ch09.x.extlib

import java.io.File
import java.io.Serializable
import java.util.Arrays

import scala.collection.mutable.{MutableList => List}
/**
 * Lists the files based on the criteria we have defined like: recursivity,
 * maximum number of files,
 *
 * @author rambabu.posa
 */
class RecursiveExtensionFilteredListerScala extends Serializable {

  var extensions = List[String]()
  var files = List[File]()
  var limit = -1
  var recursive = false
  var hasChanged = false
  var startPath: String = _

  /**
   * Adds an extension to filter. Can start with a dot or not. Is case
   * sensitive: .jpg is not the same as .JPG.
   *
   * @param extension
   * to filter on.
   */
  def addExtension(extension: String): Unit = {
    if (extension.startsWith("."))
      extensions = extensions :+ extension
    else
      extensions = extensions :+ "." + extension

    hasChanged = true
  }

  /**
   * Checks if the file is matching our constraints.
   *
   * @param dir
   * @param name
   * @return
   */
  private def check(dir: File, name: String): Boolean = {
    val f = new File(dir, name)
    if (f.isDirectory) {
      if (recursive) list0(f)
      false
    }
    else {
      import scala.collection.JavaConversions._
      for (ext <- extensions) {
        if (name.toLowerCase.endsWith(ext)) return true
      }
      false
    }
  }

  private def dir: Boolean = {
    if (startPath == null) return false
    list0(new File(startPath))
  }

  /**
   * Returns a list of files based on the criteria given by setters (or
   * default values)
   *
   * @return a list of files
   */
  def getFiles: List[File] = {
    if (hasChanged == true) {
      dir
      hasChanged = false
    }
    files
  }

  private def list0(folder: File): Boolean = {
    if (folder == null) return false
    if (!folder.isDirectory) return false
    val listOfFiles = folder.listFiles((dir: File, name: String) => check(dir, name)).toList
    if (listOfFiles == null) return true
    if (limit == -1)
      Arrays.asList(listOfFiles).add(files.toList)
    else {
      val fileCount = files.size
      if (fileCount >= limit) {
        recursive = false
        return false
      }
      var i = fileCount
      var j = 0
      while (i < limit && j < listOfFiles.length) {
        files = files :+ listOfFiles(j)
        i += 1
        j += 1
      }
    }
    true
  }

  /**
   * Sets the limit of files you want to list. Default is no limit.
   *
   * @param i
   */
  def setLimit(i: Int): Unit = {
    limit = i
    hasChanged = true
  }

  /**
   * Sets the starting path.
   *
   * @param newPath
   */
  def setPath(newPath: String): Unit = {
    startPath = newPath
    hasChanged = true
  }

  /**
   * Sets the recursion, default is false.
   *
   * @param b
   */
  def setRecursive(b: Boolean): Unit = {
    recursive = b
    hasChanged = true
  }

}
