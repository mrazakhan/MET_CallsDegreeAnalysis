import java.security.MessageDigest

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.math
import java.io._

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator



class Location(var lat:Double=0.0, var lon:Double=0.0) extends Serializable{
	def distance(l1:Location)={
		var R = 6371; // Radius of the earth in km
  		var dLat = (lat-l1.lat)*math.Pi/180  
  		var dLon = (lon-l1.lon)*math.Pi/180 
  		var a = 
    			math.sin(dLat/2) * math.sin(dLat/2) +
    			math.cos((lat)*math.Pi/180) * math.cos((l1.lat)*math.Pi/180) * 
    			math.sin(dLon/2) * math.sin(dLon/2)
    		 
  		var c = 2 * math.atan2(Math.sqrt(a), math.sqrt(1-a))
  		var d = R * c; // Distance in km
  		d
	}

}
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Location])
    kryo.register(classOf[CallsVolDegreeStats])
    kryo.register(classOf[DSV])
  }
}
class CallsVolDegreeStats extends Serializable{



	
def initDistrictsProvinceMap(sc:SparkContext,fileName:String,districtIndex:Int,provinceIndex:Int, delimiter:String=",")={
        var dpFile=sc.textFile(fileName).map(line=>(new DSV(line,",")))
        var dpFiltered=dpFile.filter(d=>((d.parts(0).contains("ID")==false)&&(d.parts(provinceIndex)!="")))        
        dpFiltered.map(p=>(p.parts(districtIndex),p.parts(provinceIndex))).collectAsMap()
}

def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }

/*
def firstLocation(dateLoc:Iterable[(String,String,String)])={
        var dateLocRdd=sc.parallelize(dateLoc.toList)
     dateLocRdd.map(dl=>(dl._1,(dl._2.toDouble,dl._3.toDouble))).sortByKey(true).take(1)(0)._2
}
*/

def initDistrictsLocMap(sc:SparkContext, fileName:String, districtIndex:Int,  latIndex:Int,  lngIndex:Int,delimiter:String=",")={
        var dlFile=sc.textFile(fileName).map(line=>(new DSV(line,",")))
        //var dlFiltered=dlFile.filter(d=>(d.parts(0).contains("ID")==false))
        
        dlFile.filter(p=>((p.parts(0).contains("ID")==false)&&(parseDouble(p.parts(latIndex))!=None)&&(parseDouble(p.parts(lngIndex))!=None))).map(p=>(p.parts(districtIndex),(p.parts(latIndex).toDouble,p.parts(lngIndex).toDouble))).collect()
        
}
	def initSubscriberDistrictMap(sc:SparkContext,locRDD:RDD[(String, (String, String, String, String))])={
		locRDD.map{case(k,v)=>(k,v._1)}.collectAsMap()

	}

        def initSubscriberProvinceMap(sc:SparkContext,locRDD:RDD[(String, (String, String, String, String))])={
                locRDD.map{case(k,v)=>(k,v._2)}.collectAsMap()

        }

}
object CallsVolDegreeStatsMain extends Serializable{

                val conf = new SparkConf().setMaster("yarn-client")
		//setMaster("spark://messi.ischool.uw.edu:7077")
                .setAppName("MonthlyCOGAgg")
                .set("spark.shuffle.consolidateFiles", "true")
		.set("spark.storage.blockManagerHeartBeatMs", "300000")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "MyRegistrator")
		.set("spark.akka.frameSize","120")
		.set("spark.default.parallelism","40")
		 .set("spark.executor.memory", "8g") 
		.set("spark.kryoserializer.buffer.max.mb","6024")
		.set("spark.kryoserializer.buffer.mb","512")
		val sc = new SparkContext(conf)

	def main(args:Array[String]){

		//(L01885477,Kicukiro,Kigali,-1.9707799400453185,30.094640020854737)

		val inputPath="hdfs:///user/mraza/Rwanda_In/"
		val outputPath = "hdfs:///user/mraza/Rwanda_In/9_26/CallsVolDegree/"

		var month1=args(0)//month name		
			
		val locFile1=args(1)//first month loc

		var callFile1=args(2)

	 var cds = new CallsVolDegreeStats()

         var distProvinceMap=sc.parallelize(cds.initDistrictsProvinceMap(sc,inputPath+"Districts.csv",1,2).toSeq)

        var locRDD1=sc.textFile(inputPath+locFile1,30).map(line=>(new DSV(line.replace("(","").replace(")",""),","))).map(d=>(d.parts(0),(d.parts(1),d.parts(2),d.parts(3),d.parts(4))))
    
       var subsDistMap1=cds.initSubscriberDistrictMap(sc,locRDD1)//Subscriber vs Dist map for month1
    
        var subsProvMap1=cds.initSubscriberProvinceMap(sc,locRDD1)
    
        

        //Reading the call file in caller ->callee form
        var callRDD1_1=sc.textFile(inputPath+callFile1,30).map(line=>(new DSV(line,"\\|"))).map(d=>(d.parts(0),(d.parts(1))))
        callRDD1_1.count()
        //Reading the call file again in callee ->caller form
        var callRDD1_2=callRDD1_1.map{case(k,v)=>(v,k)}
        callRDD1_2.count()
        

        var callRDD1=callRDD1_1.union(callRDD1_2).map{case(k,v)=>((k,subsDistMap1.get(k).getOrElse("None"),subsProvMap1.get(k).getOrElse("None"),subsDistMap1.get(v).getOrElse("None"),subsProvMap1.get(v).getOrElse("None")),v)}

        callRDD1.count()
        

        
        var callRDD1Degree=callRDD1.distinct().mapValues(_ => 1).reduceByKey(_ + _)
        callRDD1Degree.count()

        var callRDD1Volume=sc.parallelize(callRDD1.countByKey().toSeq,20)
        callRDD1Volume.count()

        var callRDD1TotalDegree=callRDD1Degree.map{case(k,v)=>(k._1,v)}.reduceByKey(_+_)
        callRDD1TotalDegree.count()
        
        var callRDD1TotalVolume=callRDD1Volume.map{case(k,v)=>(k._1,v)}.reduceByKey(_+_)
        callRDD1TotalVolume.count()
        
        
        var Month1Stats=callRDD1Volume.join(callRDD1Degree)

        //Month1Stats.count()
        
        //(L72656815,(((0502,Bugesera,East,Bugesera,East,8,4),8),4))
        var Month1Formatted=Month1Stats.map{case(k,v)=>(k._1,(month1,k._2,k._3,k._4,k._5,v._1,v._2))}.join(callRDD1TotalVolume).join(callRDD1TotalDegree).map{case(k,v)=>(k,(v._1._1._1,v._1._1._2,v._1._1._3,v._1._1._4,v._1._1._5,v._1._1._6,v._1._1._7,v._1._2,v._2))}.coalesce(1).mapPartitions(it=>(Seq("(SubscriberId,(Month,A-District,A-Province,B-District,B-Province,A-Volume,A-Degree,A-TotalVolume,A-TotalDegree))")++it).iterator)
        
        Month1Formatted.saveAsTextFile(outputPath+month1+"CallVolDegree")

		

	}
}
