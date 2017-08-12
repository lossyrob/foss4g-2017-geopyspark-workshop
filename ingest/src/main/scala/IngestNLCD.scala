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

object IngestNLCD {
  def main(args: Array[String]): Unit = {
    val previousLayerName = "nlcd-zoomed"
    val layerName = "nlcd-zoomed-256"
    val layoutScheme = ZoomedLayoutScheme(WebMercator)

    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName("Ingest DEM")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

    implicit val sc = new SparkContext(conf)

    val reader = S3LayerReader("datahub-catalogs-us-east-1", "")
    val writer = S3LayerWriter("datahub-catalogs-us-east-1", "")
    val deleter = S3LayerDeleter("datahub-catalogs-us-east-1", "")
    val as = writer.attributeStore


    // for(z <- 0 to 12) {
    //   val lid = LayerId(layerName, z)
    //   if(as.layerExists(lid)) { deleter.delete(lid) }
    // }

    val numPartitions =
      if(args.length > 0) {
        args(0).toInt
      } else {
        5000
      }

    try {
      val source  =
        reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](
          LayerId(previousLayerName, 12), numPartitions=numPartitions
        ).asRasters.map { case (_, raster) => (ProjectedExtent(raster.extent, WebMercator), raster.tile) }


      val (zoom, md) = source.collectMetadata[SpatialKey](layoutScheme)

      val tiles = ContextRDD(source.tileToLayout[SpatialKey](md), md)

      Pyramid.levelStream(tiles, layoutScheme, zoom, 0).foreach { case (z, layer) =>
        val lid = LayerId(layerName, z)
        writer.write(lid, layer, ZCurveKeyIndexMethod)
      }
    } finally {
      sc.stop()
    }
  }
}
