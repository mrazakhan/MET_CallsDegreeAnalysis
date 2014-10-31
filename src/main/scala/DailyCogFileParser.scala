import java.security.MessageDigest

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Hex {

  def valueOf(buf: Array[Byte]): String = buf.map("%02X" format _).mkString
}


class DSV (var line:String="", var delimiter:String=",",var parts:Array[String]=Array("")) extends Serializable {

	 parts=line.split(delimiter,-1)

def hasValidVal(index: Int):Boolean={
    return (parts(index)!=null)&&(parts.length>index)
}
def contains(text:String):Boolean={
    
    for(i <- 1 to (parts.length-1))
        if(parts(i).contains(text))
            return false
            
    true
}

}

