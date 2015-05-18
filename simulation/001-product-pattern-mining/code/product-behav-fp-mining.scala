import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}

//val distFile = sc.textFile("/user/hduser/sanjay/margin-mgmt/fm_rchg_data_unq_wo_msisdn/*")
val distFile = sc.textFile("/user/hduser/sanjay/margin-mgmt/fm_rchg_non_tt_unq_kel_nov/*")
val rchg_pattern: RDD[Array[String]] = distFile.map( x => x.split(","))

val fpg = new FPGrowth()
.setMinSupport(0.01)
.setNumPartitions(10)
val model = fpg.run(rchg_pattern)

model.freqItemsets.collect().foreach { itemset =>
  println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
}

