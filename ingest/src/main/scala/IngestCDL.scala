package ingest

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.kryo._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.index._
import geotrellis.spark.tiling._
import geotrellis.spark.pyramid._
import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.serializer.KryoSerializer

object IngestCDL {
  def main(args: Array[String]): Unit = {
    val cdlBucket = "datahub-rawdata-us-east-1"
    val cdlKey = "cdl/CDLS_2016_30m.tif"
    val layerName = "cdl-2016-zoomed"
    val layoutScheme = ZoomedLayoutScheme(WebMercator)

    val writer = S3LayerWriter("datahub-catalogs-us-east-1", "")
    val deleter = S3LayerDeleter("datahub-catalogs-us-east-1", "")
    val as = writer.attributeStore


    val numPartitions =
      if(args.length > 0) {
        args(0).toInt
      } else {
        5000
      }

    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName("Ingest DEM")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

    implicit val sc = new SparkContext(conf)

    try {
      val source: RDD[(ProjectedExtent, Tile)] =
        S3GeoTiffRDD.singleband[ProjectedExtent](
          cdlBucket,
          cdlKey,
          S3GeoTiffRDD.Options(
            maxTileSize = Some(1024),
            numPartitions = Some(numPartitions),
            partitionBytes = None
          )
        )

      val md = {
        val (_, md) = source.collectMetadata[SpatialKey](FloatingLayoutScheme(1024))
        md.copy(cellType = UByteConstantNoDataCellType)
      }

      val floating = ContextRDD(source.tileToLayout[SpatialKey](md), md)
      val (zoom, tiles) = floating.reproject(WebMercator, layoutScheme)

      Pyramid.levelStream(tiles, layoutScheme, zoom, 0).foreach { case (z, layer) =>
        val lid = LayerId(layerName, z)
        writer.write(lid, layer, ZCurveKeyIndexMethod)
      }
    } finally {
      sc.stop()
    }
  }
}
